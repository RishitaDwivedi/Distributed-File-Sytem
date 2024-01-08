Distributed File System Coursework
This repository contains the coursework for COMP2207, which involves the creation of a distributed storage system using Java. The system consists of a Controller and multiple Data Stores (Dstores) that support concurrent client operations such as storing, loading, listing, and removing files.

System Architecture

Controller: Manages Dstores and client requests, maintaining an index for the allocation of files across Dstores.
Dstores: Stores the actual files and communicates with the Controller.
Client: Interacts with the system through the Controller to perform file operations.

Networking

Communication over TCP connections.
Dstores listen on different ports on the same machine.
