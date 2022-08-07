package org.kefir.sql

public interface Closeable {
    public fun close()
}

public fun <T : Closeable> T.use(handler: (T) -> Unit) {
    try {
        handler(this)
    } finally {
        this.close()
    }
}
