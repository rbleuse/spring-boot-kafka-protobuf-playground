plugins {
	kotlin("jvm") version "2.4.0"
	kotlin("plugin.spring") version "2.4.0"
	id("org.springframework.boot") version "4.2.0-SNAPSHOT"
	id("io.spring.dependency-management") version "1.1.7"
	id("com.google.protobuf") version "0.10.0"
	id("org.jmailen.kotlinter") version "5.6.0"
}

group = "io.github.rbleuse"
version = "0.0.1-SNAPSHOT"

java {
	toolchain {
		languageVersion = JavaLanguageVersion.of(25)
	}
}

repositories {
	maven("https://repo.spring.io/snapshot")
	mavenCentral()
}

dependencies {
	implementation("org.springframework.boot:spring-boot-starter-web")
	implementation("org.springframework.boot:spring-boot-kafka")
	implementation("com.google.protobuf:protobuf-java")
	implementation("org.jetbrains.kotlin:kotlin-reflect")
	developmentOnly("org.springframework.boot:spring-boot-docker-compose")
	testImplementation("org.springframework.boot:spring-boot-starter-test") {
		exclude(group = "org.mockito", module = "mockito-core")
	}
	testImplementation("com.ninja-squad:springmockk:5.0.1")
	testImplementation("org.testcontainers:testcontainers-kafka")
	testImplementation("org.testcontainers:testcontainers-junit-jupiter")
	testImplementation("io.kotest:kotest-assertions-core-jvm:6.2.1")
	testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

kotlin {
	compilerOptions {
		freeCompilerArgs.addAll("-Xjsr305=strict")
	}
}

tasks.withType<Test> {
	useJUnitPlatform()
}
