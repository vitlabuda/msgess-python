# MsgESS
The **MsgESS** (Message Exchange over Stream Sockets) [messages] is a **Python 3** library and network protocol which allows applications to send and receive different types of data (raw binary data, UTF-8 strings, JSON, ...) in the form of messages reliably over any stream socket (a socket with the SOCK_STREAM type, e.g. TCP or Unix sockets). Each message can be assigned a message class that allows the app using the library to multiplex message channels.


## Requirements
Tested and works in Python 3.7. No external dependencies are required.


## Usage
The package is located in the `msgess` directory. All the library's functionality is contained within the `MsgESS` class. Its public API is documented using docstrings and type annotations; see the [msgess.py](msgess/msgess.py) file.

Usage examples can be found in the [tests](tests) directory.


## Other versions
The library is also available for these programming languages, with the network protocol being interoperable between them:
* [Java](https://github.com/vitlabuda/msgess-java)


## Licensing
This project is licensed under the 3-clause BSD license. See the [LICENSE](LICENSE) file for details.

Written by [Vít Labuda](https://vitlabuda.cz/).
