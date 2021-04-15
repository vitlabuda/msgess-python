# MsgESS
The **MsgESS** (Message Exchange over Stream Sockets) [messages] is a **Python 3** library and network protocol which allows applications to send and receive different types of data (raw binary data, UTF-8 strings, JSON, ...) reliably over any stream socket (a socket with the SOCK_STREAM type, e.g. TCP or Unix sockets) in the form of messages.


## Requirements
Tested and works in Python 3.7. No external dependencies are required.


## Usage
The package is located in the `msgess` directory. All the library's functionality is contained within the `MsgESS` class. Its public API is documented using docstrings and type annotations; see the [msgess.py](msgess/msgess.py) file.

Usage examples can be found in the [tests](tests) directory. 


## Licensing
This project is licensed under the 3-clause BSD license. See the [LICENSE](LICENSE) file for details.
