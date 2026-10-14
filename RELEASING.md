# 发布指南

[中文](RELEASING.md) | [English](RELEASING.en.md)

本文说明 nasa-kafka 的公开发布条件和产物合同。已公开版本的内容不可原地替换，发布必须对应已确认的提交、版本和完整归档。

## 发布前条件

- Central Portal 当前发布账户中存在状态为 `Verified`、且覆盖 `io.github.nasa-runtime` 的 namespace。
  仓库文档不能替代 Portal 的实际授权状态，发布前必须在 Portal 页面确认。
- Central Portal 已生成 User Token。
- 本机已配置 GPG 主签名密钥，公钥已上传至 Central 支持的公开 key server。
- 发布范围只包含目标组件及直接关联内容，`pom.xml`、README 依赖示例和最终 POM 中的版本一致。
- 完成提交并推送目标分支，远端 CI 全部通过，远端提交 SHA 与候选归档来源一致。
- 获得维护者对版本、分支和公开发布动作的明确授权后才上传。
- 质量工程仅本地使用、不进入公开仓库或产品归档。产品不得依赖本地质量入口。

Central 凭证放在用户级 `~/.m2/settings.xml`，不要写入项目：

```xml
<settings>
    <servers>
        <server>
            <id>central</id>
            <username>${env.CENTRAL_USERNAME}</username>
            <password>${env.CENTRAL_PASSWORD}</password>
        </server>
    </servers>
</settings>
```

GPG 口令交给 `gpg-agent`，不要用 `-Dgpg.passphrase=`——那会进入 shell 历史和进程列表。

macOS 上建议安装 `pinentry-mac` 并在 `~/.gnupg/gpg-agent.conf` 中配置：

```text
pinentry-program /opt/homebrew/bin/pinentry-mac
default-cache-ttl 10800
max-cache-ttl 10800
```

注意 `GNUPGHOME` 不要指向层级很深的目录：gpg-agent 的 Unix socket 路径上限约 104 字符，超长会报
`can't connect to the gpg-agent: File name too long`。

## 文档与本地产物

```bash
mvn -B -ntp clean verify
```

README 首屏与独立架构章节必须解释多数据源和 lane 隔离、Java/Rust 共同合同、确认时刻、事务原子范围、有限任务恢复条件、容量及运维观测；POM description、Javadoc、示例和配置含义保持一致。构建成功不替代这些内容的核对。

中英文文档的默认值、入口和失败边界必须一致。主 JAR 与 sources JAR 的 `META-INF/`、Javadoc JAR 的 `doc-files/` 均包含中英文 README、接入合同、贡献指南、安全策略、发布指南和许可证，逐个与候选文件核对。

`target/` 中必须至少存在：

- `nasa-kafka-1.0.0.jar`
- `nasa-kafka-1.0.0-sources.jar`
- `nasa-kafka-1.0.0-javadoc.jar`

直接打开主 JAR、sources JAR 和 Javadoc JAR，核对文件清单、版本、许可证、当前源码和渲染后的类型/API 文档。主 JAR 与 sources JAR 中的 `META-INF/README.md` 必须与候选 README 逐字节一致，POM 的坐标、依赖和 description 必须准确；不包含本地质量工程、日志、密钥或本机元数据。编译字节码为 Java 21。

同时核对功能包结构：根包包含命名目录、资源所有者和业务句柄，公开类型与方法说明完整，Javadoc 链接可以定位。自动配置资源应指向 `io.github.nasaruntime.kafka.spring.NasaKafkaConfig`，配置转换使用的 `flat-source-defaults.json` 应位于 `io/github/nasaruntime/kafka/config/`。源码归档只包含实际源码及声明的产品资源，不能残留原平铺包路径的类。

逐一阅读 README、接入合同、安全策略、贡献指南、类型与方法 Javadoc、POM、示例和关联组件引用；检查功能包路径、权限、确认与关闭语义一致，链接指向存在的文件或标题。公开文本不得使用问题处置标签、内部阶段编号、日期批次、自动化助手归因或本机执行记录；真实 API、协议名称及依赖版本保留原名，不能为清理文字而改变其含义。

公开文本只说明当前合同，不写入内部执行记录或阶段标签。既有版本归档存在遗漏时，本地改文档不代表线上内容已改变，需要独立版本和相应发布授权。

## 上传与回读

```bash
export CENTRAL_USERNAME='<token username>'
export CENTRAL_PASSWORD='<token password>'
mvn -B -ntp -Pcentral-release clean deploy
```

`central-release` profile 会为 POM、主 JAR、sources JAR 和 Javadoc JAR 生成 GPG 签名，
并由 Central Publishing Maven Plugin 生成校验和、上传 deployment。
配置默认不自动发布；上传验证通过后，必须在 Central Portal 再次核对坐标和文件，再手动点击 Publish。

公开后回读 registry 元数据和可下载的 POM/JAR，确认版本、README、许可证与本地候选一致；不能仅凭上传成功确认内容正确。

## GitHub Release

Central 发布成功后，使用 `git tag -s` 创建经批准版本的签名标签，再通过 `git push origin` 推送该标签。

标签必须指向上述已核对的提交。GitHub Release 的能力与兼容性说明以当前 README 和 [接入合同](CHANGELOG.md) 为准。
不要把本机签名私钥、Central Token 或用户级 Maven 配置作为附件上传。
