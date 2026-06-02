plugins {
	kotlin("jvm") version "2.3.21"
	kotlin("plugin.spring") version "2.3.21"
	id("org.springframework.boot") version "4.1.0-RC1"
	id("io.spring.dependency-management") version "1.1.7"
	id("com.google.protobuf") version "0.10.0"
}

group = "io.github.rbleuse"
version = "0.0.1-SNAPSHOT"

val protobufVersion = "4.35.0"

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
	implementation("com.google.protobuf:protobuf-java:$protobufVersion")
	implementation("org.jetbrains.kotlin:kotlin-reflect")
	developmentOnly("org.springframework.boot:spring-boot-docker-compose")
	testImplementation("org.springframework.boot:spring-boot-starter-test")
	testImplementation("org.springframework.kafka:spring-kafka-test")
	testImplementation("org.testcontainers:testcontainers-kafka")
	testImplementation("org.testcontainers:testcontainers-junit-jupiter")
	testImplementation("io.kotest:kotest-assertions-core-jvm:6.1.0")
	testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

protobuf {
	protoc {
		artifact = "com.google.protobuf:protoc:$protobufVersion"
	}
	// Spring Boot 4.1.0-RC1 injects an unused gRPC generator without a managed version; remove it from the tool locator and generated tasks.
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
