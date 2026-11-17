# Implementation and verification

1. Update example metadata, uv selection, CI runtimes, integration setup and guides.
2. Compile Python sources and run the existing script unit tests under Python 3.14.
3. Verify rejection of an older reused integration environment and build strict docs.

Full Redis, external application and ML integration reruns are outside these local checks.

Validation on Python 3.14.10: all 15 existing script unit tests passed; Python
sources compiled; a reused Python 3.13 environment was rejected before pytest;
`mkdocs build --strict` and `git diff --check` passed.
