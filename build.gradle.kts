plugins {
    java
}

allprojects {
    group = "com.distributed.store"
    version = "1.0-SNAPSHOT"

    repositories {
        mavenCentral()
    }
}

subprojects {
    apply(plugin = "java")

    java {
        toolchain {
            languageVersion.set(JavaLanguageVersion.of(25)) // Using modern Java (Virtual Threads!)
        }
    }

    dependencies {
        // Logging (Essential for distributed debugging)
        implementation("org.slf4j:slf4j-api:2.0.17")
        implementation("ch.qos.logback:logback-classic:1.5.23")

        // Testing
        testImplementation("org.junit.jupiter:junit-jupiter-api:6.0.1")
        testImplementation("org.assertj:assertj-core:3.27.6")
    }

    tasks.test {
        useJUnitPlatform()
    }
}