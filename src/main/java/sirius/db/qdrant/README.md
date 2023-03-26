# qdrant Database API

Provides a simple API to access the [qdrant](https://qdrant.tech/) database.

Qdrant is a fast and scalable vector search engine. A common AI task is to convert text or images into vectors
using embeddings. These vectors are then stored in a database. During retrieval either an example text, image or
query is also transformed into a vector and the task of the vector database is to find nearest neighbors in a
quick and efficient manner.

The implementation is lighwieght and does not require any external dependencies as all requests are sent via
the JSON / HTTP interface.
