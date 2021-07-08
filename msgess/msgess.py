#!/usr/bin/python3
# SPDX-License-Identifier: BSD-3-Clause
#
# Copyright (c) 2021 Vít Labuda. All rights reserved.
#
# Redistribution and use in source and binary forms, with or without modification, are permitted provided that the
# following conditions are met:
#  1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following
#     disclaimer.
#  2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the
#     following disclaimer in the documentation and/or other materials provided with the distribution.
#  3. Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote
#     products derived from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
# INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
# SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
# WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


from __future__ import annotations
from typing import Optional, Tuple, Union
import abc
import gc
import socket
import json
import gzip
import zlib


class MsgESS:
    """
    The MsgESS (Message Exchange over Stream Sockets) [messages] is a library and network protocol which allows
    applications to send and receive different types of data (raw binary data, UTF-8 strings, JSON, ...) in the form
    of messages reliably over any stream socket (a socket with the SOCK_STREAM type, e.g. TCP or Unix sockets). Each
    message can be assigned a message class that allows the app using the library to multiplex message channels.

    Tested and works in Python 3.7.
    """

    class MsgESSException(Exception):
        """The only exception raised by MsgESS's methods."""

        def __init__(self, message: str, original_exception: Optional[Exception] = None):
            super().__init__(message)
            self.original_exception: Optional[Exception] = original_exception

    class StreamSocketLikeObject(metaclass=abc.ABCMeta):
        """
        An object implementing the recv() and sendall() method that can be passed to the constructor instead of a
        socket.socket object.
        """

        @abc.abstractmethod
        def recv(self, n: int) -> bytes:
            raise NotImplementedError("The recv() method must be overridden prior to using it!")

        @abc.abstractmethod
        def sendall(self, data: bytes) -> None:
            raise NotImplementedError("The sendall() method must be overridden prior to using it!")

    class _MessageDataType:
        """Contains the integer constants used to denote the data type in messages."""

        BINARY: int = 1
        STRING: int = 2
        JSON_ARRAY: int = 3
        JSON_OBJECT: int = 4

    LIBRARY_VERSION: int = 6
    PROTOCOL_VERSION: int = 3

    def __init__(self, socket_: Union[socket.socket, StreamSocketLikeObject]):
        """Initializes a new MsgESS instance.

        :param socket_: The socket or stream-socket-like object to receive and send messages through.
        """

        self._socket: Union[socket.socket, MsgESS.StreamSocketLikeObject] = socket_
        self._compress_messages: bool = True
        self._max_message_size: int = 25000000  # in bytes

    def get_socket(self) -> Union[socket.socket, StreamSocketLikeObject]:
        """Gets the socket or stream-socket-like object passed to __init__.

        :return: The socket or socket-like object passed to __init__.
        """

        return self._socket

    def set_compress_messages(self, compress_messages: bool) -> None:
        """Turns the message compression on or off.

        :param compress_messages: Turn the message compression on or off.
        """

        self._compress_messages = compress_messages

    def set_max_message_size(self, max_message_size: int) -> None:
        """Set the maximum accepted message size while receiving.

        :param max_message_size: The new maximum message size in bytes.
        :raises: MsgESS.MsgESSException: If the specified maximum message size is negative.
        """

        if max_message_size < 0:
            raise MsgESS.MsgESSException("The new maximum message size is invalid!")

        self._max_message_size = max_message_size

    def send_binary_data(self, binary_data: bytes, message_class: int, _data_type: int = _MessageDataType.BINARY) -> None:
        """Send a message with binary data in its body to the socket.

        :param binary_data: The data to send.
        :param message_class: User-defined message class that can be used for multiplexing.
        :param _data_type: Used internally - DO NOT SET!
        :raises: MsgESS.MsgESSException: If any error is encountered during the sending process.
        """

        if not isinstance(binary_data, bytes):
            raise MsgESS.MsgESSException("The data sent must be of the 'bytes' type!")

        # compress message, if requested
        if self._compress_messages:
            binary_data = gzip.compress(binary_data)
            gc.collect()

        binary_data_length = len(binary_data)

        # assemble message:
        #  message header = magic string (11b), protocol version (4b), raw bytes length (4b), user-defined message class (4b),
        #   is message compressed? (1b), data type (1b) -> 25 bytes in total
        #  message footer = magic string (9b) -> 9 bytes in total
        message = b"MsgESSbegin"
        message += self.PROTOCOL_VERSION.to_bytes(4, byteorder="big", signed=True)
        message += binary_data_length.to_bytes(4, byteorder="big", signed=True)
        message += message_class.to_bytes(4, byteorder="big", signed=True)
        message += self._compress_messages.to_bytes(1, byteorder="big", signed=True)
        message += _data_type.to_bytes(1, byteorder="big", signed=True)
        message += binary_data
        message += b"MsgESSend"

        binary_data = None
        gc.collect()

        # send message
        try:
            self._socket.sendall(message)
        except OSError as e:
            raise MsgESS.MsgESSException("Failed to send the message to the socket!", e)

    def receive_binary_data(self, _data_type: int = _MessageDataType.BINARY) -> Tuple[bytes, int]:
        """Receive a message with binary data in its body from the socket. Blocks until a full message is received.

        :param _data_type: Used internally - DO NOT SET!
        :return: The received binary data and message class.
        :raises: MsgESS.MsgESSException: If any error is encountered during the receiving process.
        """

        # receive, parse and check message header (see self.send_binary_data for header items and their lengths)
        header = self._receive_n_bytes_from_socket(25)
        if header[0:11] != b"MsgESSbegin":
            raise MsgESS.MsgESSException("The received message has an invalid magic header string!")

        if int.from_bytes(header[11:15], byteorder="big", signed=True) != self.PROTOCOL_VERSION:
            raise MsgESS.MsgESSException("The remote host uses an incompatible protocol version!")

        message_length = int.from_bytes(header[15:19], byteorder="big", signed=True)
        if message_length < 0:
            raise MsgESS.MsgESSException("The received message's length is invalid!")
        if message_length > self._max_message_size:
            raise MsgESS.MsgESSException("The received message is too big!")

        message_class = int.from_bytes(header[19:23], byteorder="big", signed=True)
        if message_class < 0:
            raise MsgESS.MsgESSException("The received message's class is invalid!")

        is_message_compressed = bool.from_bytes(header[23:24], byteorder="big", signed=True)

        # check the data type
        if int.from_bytes(header[24:25], byteorder="big", signed=True) != _data_type:
            raise MsgESS.MsgESSException("The received message has an invalid data type!")

        # receive and possibly decompress message body
        message = self._receive_n_bytes_from_socket(message_length)
        if is_message_compressed:
            try:
                message = gzip.decompress(message)
            except (OSError, EOFError, zlib.error) as e:
                raise MsgESS.MsgESSException("Failed to decompress the received message's body!", e)
            gc.collect()

        # receive and check message footer
        footer = self._receive_n_bytes_from_socket(9)
        if footer != b"MsgESSend":
            raise MsgESS.MsgESSException("The received message has an invalid magic footer string!")

        return message, message_class

    def send_string(self, string: str, message_class: int, _data_type: int = _MessageDataType.STRING) -> None:
        """Send a message with an UTF-8 string in its body to the socket.

        :param string: The string to send.
        :param message_class: User-defined message class that can be used for multiplexing.
        :param _data_type: Used internally - DO NOT SET!
        :raises: MsgESS.MsgESSException: If any error is encountered during the sending process.
        """

        if not isinstance(string, str):
            raise MsgESS.MsgESSException("The data sent must be of the 'str' type!")

        try:
            message = string.encode("utf-8")
        except UnicodeEncodeError as e:
            raise MsgESS.MsgESSException("The sent message's body has an invalid UTF-8 character in it!", e)

        self.send_binary_data(message, message_class, _data_type=_data_type)

    def receive_string(self, _data_type: int = _MessageDataType.STRING) -> Tuple[str, int]:
        """Receive a message with an UTF-8 string in its body from the socket. Blocks until a full message is received.

        :param _data_type: Used internally - DO NOT SET!
        :return: The received string and message class.
        :raises: MsgESS.MsgESSException: If any error is encountered during the receiving process.
        """

        message, message_class = self.receive_binary_data(_data_type=_data_type)

        try:
            return message.decode("utf-8"), message_class
        except UnicodeDecodeError as e:
            raise MsgESS.MsgESSException("The received message's body has an invalid UTF-8 character in it!", e)

    def send_json_array(self, json_array: list, message_class: int) -> None:
        """Send a message with a serialized JSON array in its body to the socket.

        :param json_array: The JSON array to serialize and send.
        :param message_class: User-defined message class that can be used for multiplexing.
        :raises: MsgESS.MsgESSException: If any error is encountered during the sending process.
        """

        if not isinstance(json_array, list):
            raise MsgESS.MsgESSException("The data sent must be of the 'list' type!")

        try:
            message = json.dumps(json_array)
        except TypeError as e:
            raise MsgESS.MsgESSException("Failed to serialize the supplied list to JSON array!", e)

        self.send_string(message, message_class, _data_type=self._MessageDataType.JSON_ARRAY)

    def receive_json_array(self) -> Tuple[list, int]:
        """
        Receive a message with a serialized JSON array in its body from the socket. Blocks until a full message is received.

        :return: The received deserialized JSON array and message class.
        :raises: MsgESS.MsgESSException: If any error is encountered during the receiving process.
        """

        message, message_class = self.receive_string(_data_type=self._MessageDataType.JSON_ARRAY)

        try:
            deserialized_json = json.loads(message)
        except json.JSONDecodeError as e:
            raise MsgESS.MsgESSException("Failed to decode the received JSON array!", e)

        if not isinstance(deserialized_json, list):
            raise MsgESS.MsgESSException("The received message doesn't contain a JSON array!")

        return deserialized_json, message_class

    def send_json_object(self, json_object: dict, message_class: int) -> None:
        """Send a message with a serialized JSON object in its body to the socket.

        :param json_object: The JSON object to serialize and send.
        :param message_class: User-defined message class that can be used for multiplexing.
        :raises: MsgESS.MsgESSException: If any error is encountered during the sending process.
        """

        if not isinstance(json_object, dict):
            raise MsgESS.MsgESSException("The data sent must be of the 'dict' type!")

        try:
            message = json.dumps(json_object)
        except TypeError as e:
            raise MsgESS.MsgESSException("Failed to serialize the supplied list to JSON array!", e)

        self.send_string(message, message_class, _data_type=self._MessageDataType.JSON_OBJECT)

    def receive_json_object(self) -> Tuple[dict, int]:
        """
        Receive a message with a serialized JSON object in its body from the socket. Blocks until a full message is received.

        :return: The received deserialized JSON object and message class.
        :raises: MsgESS.MsgESSException: If any error is encountered during the receiving process.
        """

        message, message_class = self.receive_string(_data_type=self._MessageDataType.JSON_OBJECT)

        try:
            deserialized_json = json.loads(message)
        except json.JSONDecodeError as e:
            raise MsgESS.MsgESSException("Failed to decode the received JSON object!", e)

        if not isinstance(deserialized_json, dict):
            raise MsgESS.MsgESSException("The received message doesn't contain a JSON object!")

        return deserialized_json, message_class

    def _receive_n_bytes_from_socket(self, n: int) -> bytes:
        bytes_left = n
        data = bytes()

        while bytes_left > 0:
            try:
                current_data = self._socket.recv(min(16384, bytes_left))
            except OSError as e:
                raise MsgESS.MsgESSException("Failed to receive data from the socket!", e)

            if not current_data:
                raise MsgESS.MsgESSException("The recv() call has succeeded, but no data were received - the connection is probably dead.")

            data += current_data
            bytes_left -= len(current_data)

        if n != len(data):
            raise RuntimeError("The OS has received a different number of bytes than it was requested!")

        return data
