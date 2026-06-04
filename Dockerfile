# ==========================================
# 第一階段：Maven 編編譯建構階段 (Build Stage)
# ==========================================
FROM maven:3.9.6-eclipse-temurin-17 AS builder
WORKDIR /app

# 1. 複製 pom.xml 並預先下載相依套件 (利用 Docker 快取層優化後續打包速度)
COPY pom.xml .
RUN mvn dependency:go-offline -B

# 2. 複製專案原始碼並進行實體打包 (跳過單元測試以加速建構流程)
COPY src ./src
RUN mvn clean package -DskipTests

# ==========================================
# 第二階段：運行端輕量化階段 (Run Stage)
# ==========================================
FROM eclipse-temurin:17-jre-alpine
WORKDIR /app

# 1. 從第一階段的 builder 容器中，將打包好的 jar 檔複製過來
COPY --from=builder /app/target/kafka-cqrs-demo-*.jar app.jar

# 2. 宣告運行時的容器內部埠口 (對應 application.yml 設定的 8081)
EXPOSE 8081

# 3. 配置高併發 JVM 核心參數（固定堆記憶體，防止記憶體膨脹觸發 OOM Killer）
ENTRYPOINT ["java", "-Xms512m", "-Xmx512m", "-Dfile.encoding=UTF-8", "-jar", "app.jar"]