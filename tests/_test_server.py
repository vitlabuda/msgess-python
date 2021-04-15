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


import sys
import socket

sys.path.insert(0, "../")
from msgess import MsgESS


class MsgESSCallbacker:
    def __init__(self, addr: str, port: int):
        self._addr: str = addr
        self._port: int = port

    def msgess_callback(self, msgess_: MsgESS, json_data: MsgESS.JSONType) -> MsgESS.CallbackReturnType:
        stringified_data = str(json_data)

        print("Message received:", stringified_data)
        msgess_.send_json_msg({
            "message_from": [self._addr, self._port],
            "stringified_data": stringified_data
        })

        if json_data["close_connection"]:
            return MsgESS.CallbackReturnType.EXIT_CALLBACK_LOOP

        return MsgESS.CallbackReturnType.WAIT_FOR_ANOTHER_MESSAGE


def main():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind(("127.0.0.1", 5568))
        server_socket.listen(32)

        while True:
            print("Waiting for a client to connect...")

            client_socket, client_addr = server_socket.accept()

            print("Client connected:", client_addr)

            msgess_ = MsgESS(client_socket)
            msgess_.receive_json_msgs_to_cb(MsgESSCallbacker(*client_addr).msgess_callback)
            msgess_.close_connection()

            print()


if __name__ == '__main__':
    main()
