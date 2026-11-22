package com.northeastern.cs5010.bench;

/** One generated client operation. */
public record Operation(Kind kind, int key) {

    public enum Kind {
        READ, WRITE
    }
}
