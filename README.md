Azurite is an emulator of the Azure API for testing. We use it in ClickHouse tests to validate the integration with Azure Blob Storage.

But that implementation appears to be slow under load, and one of the suspected reasons was the fact that it is implemented in Typescript.

So I thought, what happens if we vibe-code its rewrite in Rust? The code is guaranteed to be garbage, but it could be enough to facilitate simple regression testing.

Please don't be annoyed by the fact that your favorite language, Rust, is now a target for AI slop.
