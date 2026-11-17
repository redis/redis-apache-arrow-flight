# Specification

- Example script metadata requires Python >=3.14.
- uv defaults and Python CI jobs select Python 3.14.
- All integration profiles select Python 3.14, including Kubeflow component images.
- Reused integration environments below Python 3.14 fail before executing tests.
- Current setup guides state the minimum; old test results remain historical evidence.
