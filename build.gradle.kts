plugins {
	kotlin("jvm") version "2.4.0"
	kotlin("plugin.spring") version "2.4.0"
	id("org.springframework.boot") version "4.1.0-RC1"
	id("io.spring.dependency-management") version "1.1.7"
	id("com.google.protobuf") version "0.10.0"
	id("org.jmailen.kotlinter") version "5.5.0"
}

group = "io.github.rbleuse"
version = "0.0.1-SNAPSHOT"

java {
	toolchain {
		languageVersion = JavaLanguageVersion.of(25)
	}
}

repositories {
	mavenCentral()
}

dependencies {
	implementation("org.springframework.boot:spring-boot-starter-web")
	implementation("org.springframework.boot:spring-boot-kafka")
	implementation("com.google.protobuf:protobuf-java")
	runtimeOnly("com.google.protobuf:protobuf-java-util")
	implementation("org.jetbrains.kotlin:kotlin-reflect")
	developmentOnly("org.springframework.boot:spring-boot-docker-compose")
	testImplementation("org.springframework.boot:spring-boot-starter-test") {
		exclude(group = "org.mockito", module = "mockito-core")
	}
	testImplementation("com.ninja-squad:springmockk:5.0.1")
	testImplementation("org.testcontainers:testcontainers-kafka")
	testImplementation("org.testcontainers:testcontainers-junit-jupiter")
	testImplementation("io.kotest:kotest-assertions-core-jvm:6.1.11")
	testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

protobuf {
	// This app only generates protobuf messages; skip unused gRPC generation.
	plugins {
		remove(getByName("grpc"))
	}
	generateProtoTasks {
		all().configureEach {
			plugins {
				remove(getByName("grpc"))
			}
		}
	}
}

kotlin {
	compilerOptions {
		freeCompilerArgs.addAll("-Xjsr305=strict", "-Xannotation-default-target=param-property")
	}
}

tasks.withType<Test> {
	useJUnitPlatform()
}
