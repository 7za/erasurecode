# erasurecode
A wrapper to RUST liberasure crate that handle subchunk erasure codng


Liberasurecode is a wrapper to low level erasurecoding library such as Jerasure, isa-l and so on.
In a nutshell, liberasurecode takes N input buffer, and generates M output coding buffer.
This wrapper will organize  the data and the coding differently, in order to have a better granularity to repair missing data.

Instead of having 

[----------- DATA-1 -------------]
[----------- DATA-2 -------------]
[----------- DATA-3 -------------]
[---------- PARITY-1 ------------]

We will organize generate buffer as below :

[[subchunk1-data][subchunk2-data].................] => Buffer 1
[[subchunk1-data][subchunk2-data].................] => Buffer 2
[[subchunk1-data][subchunk2-data].................] => Buffer 3
[[subchunk1-code][subchunk2-code].................] => Coding 1

The idea behind this organization is to be able to repair sub part of data
