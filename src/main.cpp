#include <cstdlib>
#include <cstring>
#include <iostream>
#include <netdb.h>
#include <string>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/types.h>

struct kafka_message {
    int32_t message_size; // Size of the message
    int32_t correlation_id; // Correlation ID
    int16_t api_version; // API version
};
void parse_correlation_id(const char* buffer, kafka_message& response, int client_fd) {
    // Assuming the correlation ID is at offset 8 in the buffer
    memcpy(&response.correlation_id, buffer+8, sizeof(response.correlation_id));
    response.message_size = htonl(0);
    write(client_fd, &response.message_size, sizeof(response.message_size));
    write(client_fd, &response.correlation_id, sizeof(response.correlation_id));
}


void parse_api_version(const char* buffer, kafka_message& response, int client_fd) {
    memcpy(&response.correlation_id, buffer+8, sizeof(response.correlation_id));
    memcpy(&response.api_version, buffer + 6, sizeof(response.api_version));
    response.message_size = htonl(0); // Convert to network byte order    
    if(response.api_version < 5 || response.api_version > 11) {
        std::cerr << "Invalid API version: " << response.api_version << std::endl;
        response.api_version = 35; // Set to 35 for invalid versions
    }
    response.api_version = htonl(response.api_version);
    
    write(client_fd, &response.message_size, sizeof(response.message_size));
    write(client_fd, &response.correlation_id, sizeof(response.correlation_id));
    write(client_fd, &response.api_version, sizeof(response.api_version));
}
int main(int argc, char* argv[]) {
    // Disable output buffering
    std::cout << std::unitbuf;
    std::cerr << std::unitbuf;
    
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        std::cerr << "Failed to create server socket: " << std::endl;
        return 1;
    }

    // Since the tester restarts your program quite often, setting SO_REUSEADDR
    // ensures that we don't run into 'Address already in use' errors
    int reuse = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
        close(server_fd);
        std::cerr << "setsockopt failed: " << std::endl;
        return 1;
    }

    struct sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(9092);

    if (bind(server_fd, reinterpret_cast<struct sockaddr*>(&server_addr), sizeof(server_addr)) != 0) {
        close(server_fd);
        std::cerr << "Failed to bind to port 9092" << std::endl;
        return 1;
    }

    int connection_backlog = 5;
    if (listen(server_fd, connection_backlog) != 0) {
        close(server_fd);
        std::cerr << "listen failed" << std::endl;
        return 1;
    }

    std::cout << "Waiting for a client to connect...\n";

    struct sockaddr_in client_addr{};
    socklen_t client_addr_len = sizeof(client_addr);

    // You can use print statements as follows for debugging, they'll be visible when running tests.
    std::cerr << "Logs from your program will appear here!\n";
    
    // Uncomment this block to pass the first stage
    // 
    int client_fd = accept(server_fd, reinterpret_cast<struct sockaddr*>(&client_addr), &client_addr_len);
    std::cout << "Client connected\n";
    kafka_message response{};
    // read from client
    char buffer[1024];
    ssize_t bytes_received = recv(client_fd, buffer, sizeof(buffer), 0);
    if (bytes_received < 0) {
        std::cerr << "Failed to receive data from client" << std::endl;
        close(client_fd);
        close(server_fd);
        return 1;
    }
    // parse_correlation_id(buffer, response, client_fd);
    parse_api_version(buffer, response, client_fd);
    std::cout << "Parsed API version: " << response.api_version << std::endl;
    close(client_fd);

    close(server_fd);
    return 0;
}