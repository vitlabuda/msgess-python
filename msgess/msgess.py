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
from typing import Any, Optional, Union, List, Dict, Callable
from enum import Enum
import socket
import json
import zlib


class MsgESS:
    """
    The MsgESS (Message Exchange over Stream Sockets) [messages] is a library and network protocol which allows
    applications to send and receive different types of data (raw binary data, UTF-8 strings, JSON, ...) reliably over
    any stream socket (a socket with the SOCK_STREAM type, e.g. TCP or Unix sockets) in the form of messages.

    Tested and works in Python 3.7.
    """

    class MsgESSException(Exception):
        """The only exception thrown by MsgESS's methods."""

        def __init__(self, message: str, original_exception: Optional[Exception] = None):
            super().__init__(message)
            self.original_exception: Optional[Exception] = original_exception

    class CallbackReturnType(Enum):
        """
        A constant from this enum class must be returned by any callback function passed to the 'receive-with-callback'
        methods of MsgESS.
        """

        WAIT_FOR_ANOTHER_MESSAGE = 1
        EXIT_CALLBACK_LOOP = 2

    JSONType = Union[str, int, float, bool, None, Dict[str, Any], List[Any]]

    LIBRARY_VERSION: int = 1
    PROTOCOL_VERSION: int = 1

    def __init__(self, socket_: socket.SocketType):
        """Initializes a new MsgESS instance.

        :param socket_:
            The socket to receive and send messages through. It can be any object that implements the
            recv(int) -> bytes, sendall(bytes) and close() methods.
        """

        self._socket: socket.SocketType = socket_
        self._compress_messages: bool = True
        self._compression_level: int = -1

    def set_message_compression(self, compress_messages: bool, compression_level: Optional[int] = None) -> None:
        """Turns the message compression on or off. Optionally, it sets the compression level.

        :param compress_messages: Turn the message compression on or off.
        :param compression_level: Set the compression level (0-9; -1 = default). None means unchanged.
        """

        self._compress_messages = compress_messages
        if compression_level is not None:
            self._compression_level = compression_level

    def close_connection(self) -> None:
        """Closes the socket passed to __init__.

        :raises: MsgESS.MsgESSException: If the OS fails to close the socket.
        """

        try:
            self._socket.close()
        except OSError as e:
            raise MsgESS.MsgESSException("Failed to close the socket!", e)

    def send_binary_msg(self, binary_data: bytes) -> None:
        """Send a message with binary data in its body to the socket.

        :param binary_data: The data to send.
        :raises: MsgESS.MsgESSException: If any error is encountered during the sending process.
        """

        # compress message, if requested
        if self._compress_messages:
            binary_data = zlib.compress(binary_data, level=self._compression_level)

        binary_data_length = len(binary_data)

        # assemble message:
        #  message header = magic string (11b), protocol version (4b), raw bytes length (4b), is message compressed? (1b) = 20 bytes in total
        #  message footer = magic string (9b) = 9 bytes in total
        message = b"MsgESSbegin"
        message += self.PROTOCOL_VERSION.to_bytes(4, byteorder="big", signed=False)
        message += binary_data_length.to_bytes(4, byteorder="big", signed=False)
        message += self._compress_messages.to_bytes(1, byteorder="big", signed=False)
        message += binary_data
        message += b"MsgESSend"

        # send message
        try:
            self._socket.sendall(message)
        except OSError as e:
            raise MsgESS.MsgESSException("Failed to send the message to the socket!", e)

    def receive_binary_msg(self) -> bytes:
        """Receive a message with binary data in its body from the socket. Blocks until a full message is received.

        :return: The received binary data.
        :raises: MsgESS.MsgESSException: If any error is encountered during the receiving process.
        """

        # receive_json_msg, parse and check message header (see self.send_string_msg for header items and their lengths)
        header = self._receive_n_bytes(20)
        if header[0:11] != b"MsgESSbegin":
            raise MsgESS.MsgESSException("The received message has an invalid header!")

        if int.from_bytes(header[11:15], byteorder="big", signed=False) != self.PROTOCOL_VERSION:
            raise MsgESS.MsgESSException("The remote host uses an incompatible protocol version!")

        message_length = int.from_bytes(header[15:19], byteorder="big", signed=False)
        is_message_compressed = bool.from_bytes(header[19:20], byteorder="big", signed=False)

        # receive_json_msg and possibly decompress message body
        message = self._receive_n_bytes(message_length)
        if is_message_compressed:
            try:
                message = zlib.decompress(message)
            except zlib.error as e:
                raise MsgESS.MsgESSException("Failed to decompress the received message's body!", e)

        # receive_json_msg and check message footer
        footer = self._receive_n_bytes(9)
        if footer != b"MsgESSend":
            raise MsgESS.MsgESSException("The received message has an invalid footer!")

        return message

    def receive_binary_msgs_to_cb(self, callback: Callable[[MsgESS, bytes], CallbackReturnType]) -> None:
        """
        Receive a message with binary data in its body from the socket and pass the message to the specified
        callback function. Blocks until a full message is received. Messages are received until the callback function
        returns MsgESS.CallbackReturnType.EXIT_CALLBACK_LOOP.

        :param callback: The callback function to call for each received message.
        :raises: MsgESS.MsgESSException: If any error is encountered during the receiving process.
        """

        while True:
            message = self.receive_binary_msg()
            if callback(self, message) == self.CallbackReturnType.EXIT_CALLBACK_LOOP:
                break

    def send_string_msg(self, string: str) -> None:
        """Send a message with an UTF-8 string in its body to the socket.

        :param string: The string to send.
        :raises: MsgESS.MsgESSException: If any error is encountered during the sending process.
        """

        try:
            message = string.encode("utf-8")
        except UnicodeEncodeError as e:
            raise MsgESS.MsgESSException("The sent message's body has an invalid UTF-8 character in it!", e)

        self.send_binary_msg(message)

    def receive_string_msg(self) -> str:
        """Receive a message with an UTF-8 string in its body from the socket. Blocks until a full message is received.

        :return: The received string.
        :raises: MsgESS.MsgESSException: If any error is encountered during the receiving process.
        """

        message = self.receive_binary_msg()

        try:
            return message.decode("utf-8")
        except UnicodeDecodeError as e:
            raise MsgESS.MsgESSException("The received message's body has an invalid UTF-8 character in it!", e)

    def receive_string_msg_to_cb(self, callback: Callable[[MsgESS, str], CallbackReturnType]):
        """
        Receive a message with an UTF-8 string in its body from the socket and pass the message to the specified
        callback function. Blocks until a full message is received. Messages are received until the callback function
        returns MsgESS.CallbackReturnType.EXIT_CALLBACK_LOOP.

        :param callback: The callback function to call for each received message.
        :raises: MsgESS.MsgESSException: If any error is encountered during the receiving process.
        """

        while True:
            message = self.receive_string_msg()
            if callback(self, message) == self.CallbackReturnType.EXIT_CALLBACK_LOOP:
                break

    def send_json_msg(self, json_data: JSONType):
        """Send a message with a serialized JSON in its body to the socket.

        :param json_data: The JSON to serialize and send.
        :raises: MsgESS.MsgESSException: If any error is encountered during the sending process.
        """

        try:
            message = json.dumps(json_data)
        except TypeError as e:
            raise MsgESS.MsgESSException("Failed to serialize the supplied data to JSON!", e)

        self.send_string_msg(message)

    def receive_json_msg(self) -> JSONType:
        """
        Receive a message with a serialized JSON in its body from the socket. Blocks until a full message is received.

        :return: The received and deserialized JSON.
        :raises: MsgESS.MsgESSException: If any error is encountered during the receiving process.
        """

        message = self.receive_string_msg()

        try:
            return json.loads(message)
        except json.JSONDecodeError as e:
            raise MsgESS.MsgESSException("Failed to decode the received JSON data!", e)

    def receive_json_msgs_to_cb(self, callback: Callable[[MsgESS, JSONType], CallbackReturnType]):
        """Receive a message with a serialized JSON in its body from the socket and pass the message to the
        specified callback function. Blocks until a full message is received. Messages are received until the callback
        function returns MsgESS.CallbackReturnType.EXIT_CALLBACK_LOOP.

        :param callback: The callback function to call for each received message.
        :raises: MsgESS.MsgESSException: If any error is encountered during the receiving process.
        """

        while True:
            message = self.receive_json_msg()
            if callback(self, message) == self.CallbackReturnType.EXIT_CALLBACK_LOOP:
                break

    def _receive_n_bytes(self, n: int) -> bytes:
        bytes_left = n
        data = bytes()

        while bytes_left > 0:
            try:
                recvd_length = self._socket.recv(min(16384, bytes_left))
            except OSError as e:
                raise MsgESS.MsgESSException("Failed to receive data from the socket!", e)

            data += recvd_length
            bytes_left -= len(recvd_length)

        if n != len(data):
            raise RuntimeError("The OS has received a different number of bytes than it was requested!")

        return data
