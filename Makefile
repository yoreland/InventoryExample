all: target/inventory-1.0.jar

target/inventory-1.0.jar: src/main/java/aws/example/inventory/*.java
	mvn package

clean:
	mvn clean

