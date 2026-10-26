# Repository migration

Migrated on 2026-10-26 from `alberttwong/redis-apache-arrow-flight` to `redis/redis-apache-arrow-flight`.

All six source branches and their Git history were copied. The source `main` was `c90a2b9a77f4a8f9bf4f072da7648439301ae69c`. The destination adds migration changes for its Go module path, public Redis ADBC dependency, build configuration and links.

All 38 issues (5 open, 33 closed) and four issue comments were copied with labels, original attribution, source links and open/closed state. GitHub assigns new numbers and timestamps when issues are copied; the original author and timestamps are recorded in each issue or comment body. Historical references in copied bodies are qualified with the source repository so they retain their original meaning.

The 32 closed pull requests and historical workflow runs remain in the [source repository](https://github.com/alberttwong/redis-apache-arrow-flight). Their merged code is included in the copied Git history. The source had no tags, published releases, milestones or Actions variables.

The Test and Release workflow files are included. Dependency downloads use the public [Redis ADBC v0.0.18 snapshot](https://github.com/redis/redis-adbc-driver/tree/v0.0.18); the old private dependency secret is no longer needed.

## Issue mapping

| Original issue | Migrated issue | State |
| --- | --- | --- |
| [#3](https://github.com/alberttwong/redis-apache-arrow-flight/issues/3) | [#1](https://github.com/redis/redis-apache-arrow-flight/issues/1) | closed |
| [#4](https://github.com/alberttwong/redis-apache-arrow-flight/issues/4) | [#2](https://github.com/redis/redis-apache-arrow-flight/issues/2) | closed |
| [#5](https://github.com/alberttwong/redis-apache-arrow-flight/issues/5) | [#3](https://github.com/redis/redis-apache-arrow-flight/issues/3) | closed |
| [#6](https://github.com/alberttwong/redis-apache-arrow-flight/issues/6) | [#4](https://github.com/redis/redis-apache-arrow-flight/issues/4) | closed |
| [#7](https://github.com/alberttwong/redis-apache-arrow-flight/issues/7) | [#5](https://github.com/redis/redis-apache-arrow-flight/issues/5) | closed |
| [#8](https://github.com/alberttwong/redis-apache-arrow-flight/issues/8) | [#6](https://github.com/redis/redis-apache-arrow-flight/issues/6) | closed |
| [#9](https://github.com/alberttwong/redis-apache-arrow-flight/issues/9) | [#7](https://github.com/redis/redis-apache-arrow-flight/issues/7) | closed |
| [#10](https://github.com/alberttwong/redis-apache-arrow-flight/issues/10) | [#8](https://github.com/redis/redis-apache-arrow-flight/issues/8) | closed |
| [#11](https://github.com/alberttwong/redis-apache-arrow-flight/issues/11) | [#9](https://github.com/redis/redis-apache-arrow-flight/issues/9) | closed |
| [#12](https://github.com/alberttwong/redis-apache-arrow-flight/issues/12) | [#10](https://github.com/redis/redis-apache-arrow-flight/issues/10) | closed |
| [#13](https://github.com/alberttwong/redis-apache-arrow-flight/issues/13) | [#11](https://github.com/redis/redis-apache-arrow-flight/issues/11) | closed |
| [#14](https://github.com/alberttwong/redis-apache-arrow-flight/issues/14) | [#12](https://github.com/redis/redis-apache-arrow-flight/issues/12) | closed |
| [#15](https://github.com/alberttwong/redis-apache-arrow-flight/issues/15) | [#13](https://github.com/redis/redis-apache-arrow-flight/issues/13) | closed |
| [#16](https://github.com/alberttwong/redis-apache-arrow-flight/issues/16) | [#14](https://github.com/redis/redis-apache-arrow-flight/issues/14) | closed |
| [#21](https://github.com/alberttwong/redis-apache-arrow-flight/issues/21) | [#15](https://github.com/redis/redis-apache-arrow-flight/issues/15) | closed |
| [#22](https://github.com/alberttwong/redis-apache-arrow-flight/issues/22) | [#16](https://github.com/redis/redis-apache-arrow-flight/issues/16) | closed |
| [#23](https://github.com/alberttwong/redis-apache-arrow-flight/issues/23) | [#17](https://github.com/redis/redis-apache-arrow-flight/issues/17) | open |
| [#28](https://github.com/alberttwong/redis-apache-arrow-flight/issues/28) | [#18](https://github.com/redis/redis-apache-arrow-flight/issues/18) | closed |
| [#29](https://github.com/alberttwong/redis-apache-arrow-flight/issues/29) | [#19](https://github.com/redis/redis-apache-arrow-flight/issues/19) | closed |
| [#30](https://github.com/alberttwong/redis-apache-arrow-flight/issues/30) | [#20](https://github.com/redis/redis-apache-arrow-flight/issues/20) | closed |
| [#31](https://github.com/alberttwong/redis-apache-arrow-flight/issues/31) | [#21](https://github.com/redis/redis-apache-arrow-flight/issues/21) | closed |
| [#32](https://github.com/alberttwong/redis-apache-arrow-flight/issues/32) | [#22](https://github.com/redis/redis-apache-arrow-flight/issues/22) | closed |
| [#35](https://github.com/alberttwong/redis-apache-arrow-flight/issues/35) | [#23](https://github.com/redis/redis-apache-arrow-flight/issues/23) | closed |
| [#36](https://github.com/alberttwong/redis-apache-arrow-flight/issues/36) | [#24](https://github.com/redis/redis-apache-arrow-flight/issues/24) | closed |
| [#37](https://github.com/alberttwong/redis-apache-arrow-flight/issues/37) | [#25](https://github.com/redis/redis-apache-arrow-flight/issues/25) | closed |
| [#38](https://github.com/alberttwong/redis-apache-arrow-flight/issues/38) | [#26](https://github.com/redis/redis-apache-arrow-flight/issues/26) | closed |
| [#42](https://github.com/alberttwong/redis-apache-arrow-flight/issues/42) | [#27](https://github.com/redis/redis-apache-arrow-flight/issues/27) | open |
| [#50](https://github.com/alberttwong/redis-apache-arrow-flight/issues/50) | [#28](https://github.com/redis/redis-apache-arrow-flight/issues/28) | open |
| [#51](https://github.com/alberttwong/redis-apache-arrow-flight/issues/51) | [#29](https://github.com/redis/redis-apache-arrow-flight/issues/29) | closed |
| [#52](https://github.com/alberttwong/redis-apache-arrow-flight/issues/52) | [#30](https://github.com/redis/redis-apache-arrow-flight/issues/30) | closed |
| [#53](https://github.com/alberttwong/redis-apache-arrow-flight/issues/53) | [#31](https://github.com/redis/redis-apache-arrow-flight/issues/31) | closed |
| [#54](https://github.com/alberttwong/redis-apache-arrow-flight/issues/54) | [#32](https://github.com/redis/redis-apache-arrow-flight/issues/32) | closed |
| [#55](https://github.com/alberttwong/redis-apache-arrow-flight/issues/55) | [#33](https://github.com/redis/redis-apache-arrow-flight/issues/33) | closed |
| [#56](https://github.com/alberttwong/redis-apache-arrow-flight/issues/56) | [#34](https://github.com/redis/redis-apache-arrow-flight/issues/34) | closed |
| [#57](https://github.com/alberttwong/redis-apache-arrow-flight/issues/57) | [#35](https://github.com/redis/redis-apache-arrow-flight/issues/35) | open |
| [#58](https://github.com/alberttwong/redis-apache-arrow-flight/issues/58) | [#36](https://github.com/redis/redis-apache-arrow-flight/issues/36) | closed |
| [#63](https://github.com/alberttwong/redis-apache-arrow-flight/issues/63) | [#37](https://github.com/redis/redis-apache-arrow-flight/issues/37) | closed |
| [#64](https://github.com/alberttwong/redis-apache-arrow-flight/issues/64) | [#38](https://github.com/redis/redis-apache-arrow-flight/issues/38) | open |
