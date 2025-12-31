dependencies {
    implementation(project(":common"))

    // If we use a Raft library (like Apache Ratis) we add it here.
    // For now, we assume we are building custom Raft logic using standard Java + Netty.
    implementation("io.netty:netty-all:4.2.9.Final")
}