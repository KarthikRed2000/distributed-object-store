plugins {
    application
}

dependencies {
    // 1. Depend on the Common module (to see RaftMessage)
    implementation(project(":common"))

    // 2. Netty for Networking
    implementation("io.netty:netty-all:4.1.101.Final")

    // 3. Logging (SLF4J API + Logback Implementation)
    implementation("org.slf4j:slf4j-api:2.0.9")
    implementation("ch.qos.logback:logback-classic:1.4.11")
}

application {
    // Defines the main class so you can run via ./gradlew run
    mainClass.set("com.distributed.store.metadata.MetadataServer")
}