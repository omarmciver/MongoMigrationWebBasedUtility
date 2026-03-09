# See https://aka.ms/customizecontainer to learn how to customize your debug container and how Visual Studio uses this Dockerfile to build your images for faster debugging.

# This stage is used when running from VS in fast mode (Default for Debug configuration)
FROM mcr.microsoft.com/dotnet/aspnet:9.0 AS base
USER $APP_UID
WORKDIR /app
EXPOSE 8080
EXPOSE 8081


# This stage is used to build the service project
FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
ARG BUILD_CONFIGURATION=Release
WORKDIR /src
COPY ["MongoMigrationWebApp/MongoMigrationWebApp.csproj", "MongoMigrationWebApp/"]
COPY ["OnlineMongoMigrationProcessor/OnlineMongoMigrationProcessor.csproj", "OnlineMongoMigrationProcessor/"]
COPY ["ResumeTokenProbeExe/ResumeTokenProbeExe.csproj", "ResumeTokenProbeExe/"]
RUN dotnet restore "./MongoMigrationWebApp/MongoMigrationWebApp.csproj"
COPY . .
WORKDIR "/src/MongoMigrationWebApp"
RUN dotnet build "./MongoMigrationWebApp.csproj" -c $BUILD_CONFIGURATION -o /app/build

# This stage is used to publish the service project to be copied to the final stage
FROM build AS publish
ARG BUILD_CONFIGURATION=Release
RUN dotnet publish "./MongoMigrationWebApp.csproj" -c $BUILD_CONFIGURATION -o /app/publish /p:UseAppHost=false

# This stage is used in production or when running from VS in regular mode (Default when not using the Debug configuration)
FROM base AS final

# Switch to root to install MongoDB tools
USER root

ARG TARGETARCH
ARG MongoDumpURL=""
ARG MongoRestoreURL=""
ENV MongoDumpURL=${MongoDumpURL}
ENV MongoRestoreURL=${MongoRestoreURL}
ENV PATH="/usr/local/bin:${PATH}"

# Install prerequisites (OS-agnostic: supports Debian/Ubuntu apt-get, Azure Linux tdnf, Alpine apk)
# MongoDB tools are glibc-linked Go binaries — they work on Debian, Ubuntu, and Azure Linux.
RUN set -eux; \
    if command -v apt-get >/dev/null 2>&1; then \
        echo ">>> Detected Debian/Ubuntu (apt-get)"; \
        apt-get update; \
        apt-get install -y --no-install-recommends \
            wget gnupg ca-certificates tar gzip \
            libcurl4 libgssapi-krb5-2 libkrb5-3 \
            libsasl2-2 libssl3 libldap-2.5-0 zlib1g; \
        apt-get clean; rm -rf /var/lib/apt/lists/*; \
    elif command -v tdnf >/dev/null 2>&1; then \
        echo ">>> Detected Azure Linux (tdnf)"; \
        tdnf install -y \
            wget ca-certificates tar gzip \
            libcurl krb5-libs cyrus-sasl openssl-libs openldap zlib; \
        tdnf clean all; \
    elif command -v apk >/dev/null 2>&1; then \
        echo ">>> Detected Alpine (apk) — installing gcompat for glibc compatibility"; \
        apk add --no-cache \
            wget ca-certificates tar gzip \
            libcurl krb5-libs cyrus-sasl-lib libldap zlib \
            gcompat; \
    else \
        echo "ERROR: No supported package manager found (need apt-get, tdnf, or apk)" >&2; \
        exit 1; \
    fi

# Install MongoDB tools.
# - If MongoDumpURL and MongoRestoreURL are both provided and different, install each tool from its own URL.
# - Otherwise, download the default single-package from MongoDB CDN.
# NOTE: The apt-based MongoDB repo install path is only used on Debian/Ubuntu when no URLs are provided.
RUN set -eux; \
    if [ -z "$MongoDumpURL" ] || [ -z "$MongoRestoreURL" ]; then \
        if [ "${TARGETARCH:-amd64}" = "arm64" ]; then \
            DUMP_DEFAULT_URL="https://fastdl.mongodb.org/tools/db/mongodb-database-tools-ubuntu2204-arm64-100.13.0.tgz"; \
            RESTORE_DEFAULT_URL="https://fastdl.mongodb.org/tools/db/mongodb-database-tools-ubuntu2204-arm64-100.13.0.tgz"; \
        else \
            DUMP_DEFAULT_URL="https://fastdl.mongodb.org/tools/db/mongodb-database-tools-ubuntu2204-x86_64-100.13.0.tgz"; \
            RESTORE_DEFAULT_URL="https://fastdl.mongodb.org/tools/db/mongodb-database-tools-ubuntu2204-x86_64-100.13.0.tgz"; \
        fi; \
        MongoDumpURL="${MongoDumpURL:-$DUMP_DEFAULT_URL}"; \
        MongoRestoreURL="${MongoRestoreURL:-$RESTORE_DEFAULT_URL}"; \
    fi; \
    if [ -n "$MongoDumpURL" ] && [ -n "$MongoRestoreURL" ] && [ "$MongoDumpURL" != "$MongoRestoreURL" ]; then \
        mkdir -p /opt/mongo-tools/dump /opt/mongo-tools/restore; \
        wget -O /tmp/mongodump-tools.tgz "$MongoDumpURL"; \
        wget -O /tmp/mongorestore-tools.tgz "$MongoRestoreURL"; \
        tar -xzf /tmp/mongodump-tools.tgz -C /opt/mongo-tools/dump; \
        tar -xzf /tmp/mongorestore-tools.tgz -C /opt/mongo-tools/restore; \
        DUMP_BIN="$(find /opt/mongo-tools/dump -type f -name mongodump | head -n 1)"; \
        RESTORE_BIN="$(find /opt/mongo-tools/restore -type f -name mongorestore | head -n 1)"; \
        test -n "$DUMP_BIN"; \
        test -n "$RESTORE_BIN"; \
        chmod +x "$DUMP_BIN" "$RESTORE_BIN"; \
        ln -sf "$DUMP_BIN" /usr/local/bin/mongodump; \
        ln -sf "$RESTORE_BIN" /usr/local/bin/mongorestore; \
    elif command -v apt-get >/dev/null 2>&1; then \
        wget -qO - https://www.mongodb.org/static/pgp/server-7.0.asc | apt-key add -; \
        echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/debian bookworm/mongodb-org/7.0 main" | tee /etc/apt/sources.list.d/mongodb-org-7.0.list; \
        apt-get update; \
        apt-get install -y mongodb-database-tools; \
        apt-get clean; rm -rf /var/lib/apt/lists/*; \
    else \
        mkdir -p /opt/mongo-tools/dump; \
        wget -O /tmp/mongodump-tools.tgz "$MongoDumpURL"; \
        tar -xzf /tmp/mongodump-tools.tgz -C /opt/mongo-tools/dump; \
        DUMP_BIN="$(find /opt/mongo-tools/dump -type f -name mongodump | head -n 1)"; \
        RESTORE_BIN="$(find /opt/mongo-tools/dump -type f -name mongorestore | head -n 1)"; \
        test -n "$DUMP_BIN"; \
        test -n "$RESTORE_BIN"; \
        chmod +x "$DUMP_BIN" "$RESTORE_BIN"; \
        ln -sf "$DUMP_BIN" /usr/local/bin/mongodump; \
        ln -sf "$RESTORE_BIN" /usr/local/bin/mongorestore; \
    fi; \
    command -v mongodump; \
    command -v mongorestore; \
    mongodump --version; \
    mongorestore --version; \
    rm -rf /tmp/mongodump-tools.tgz /tmp/mongorestore-tools.tgz

# Create the migration data directory structure that the app expects
# The app looks for ResourceDrive/home/ directory structure
RUN mkdir -p /app/migration-data/home && \
    chown -R $APP_UID:$APP_UID /app/migration-data && \
    chmod -R 755 /app/migration-data

# Switch back to non-root user for security
RUN mkdir -p /app/mongodump /tmp/mongodump && \
    chown -R $APP_UID:$APP_UID /app/mongodump /tmp/mongodump && \
    chmod -R 755 /app/mongodump /tmp/mongodump

# Switch back to non-root user for security
USER $APP_UID

WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "MongoMigrationWebApp.dll"]