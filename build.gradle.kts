plugins {
    java
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
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