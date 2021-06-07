import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
    java
    id("com.github.johnrengelman.shadow") version "6.1.0"
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

configurations {
    implementation {
        isCanBeResolved = true
    }
}

dependencies {
    implementation("io.aeron:aeron-all:1.31.1")
    implementation("org.agrona:agrona:1.10.0")
    implementation("org.awaitility:awaitility:4.0.3")
    implementation("ch.qos.logback:logback-classic:1.2.3")
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.6.0")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
}

tasks.getByName<Test>("test") {
    useJUnitPlatform()
}

tasks {
    register<ShadowJar>("rsmClient") {
        destinationDirectory.set(buildDir)
        archiveFileName.set("rsm-client.jar")
        manifest {
            attributes["Main-Class"] = "rsm.client.ReplicationStateMachineMain"
        }
        from(sourceSets.main.get().output)
        from(project.configurations.implementation)
    }

    register<ShadowJar>("rsmNode") {
        destinationDirectory.set(buildDir)
        archiveFileName.set("rsm-node.jar")
        manifest {
            attributes["Main-Class"] = "rsm.node.ReplicatedStateMachineClusterNodeMain"
        }
        from(sourceSets.main.get().output)
        from(project.configurations.implementation)
    }
}