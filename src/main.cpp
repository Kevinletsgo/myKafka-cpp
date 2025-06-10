#include <cstdlib>
#include <cstring>
#include <iostream>
#include <netdb.h>
#include <string>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <vector>

using namespace std;
// struct kafka_message {
//     int32_t message_size; // Size of the message
//     int32_t correlation_id; // Correlation ID
//     int16_t api_version; // API version
// };
// void parse_correlation_id(const char* buffer, kafka_message& response, int client_fd) {
//     // Assuming the correlation ID is at offset 8 in the buffer
//     memcpy(&response.correlation_id, buffer+8, sizeof(response.correlation_id));
//     response.message_size = htonl(0);
//     write(client_fd, &response.message_size, sizeof(response.message_size));
//     write(client_fd, &response.correlation_id, sizeof(response.correlation_id));
// }


// void parse_api_version(const char* buffer, kafka_message& response, int client_fd) {
//     // Assuming buffer layout matches Kafka protocol:
//     // - API version is at offset 6 (2 bytes)
//     // - Correlation ID is at offset 8 (4 bytes)
//     memcpy(&response.api_version, buffer + 6, sizeof(response.api_version));
//     memcpy(&response.correlation_id, buffer + 8, sizeof(response.correlation_id));
    
//     // Convert from network byte order if needed
//     response.api_version = ntohs(response.api_version);  // Assuming 2-byte API version
//     response.correlation_id = ntohl(response.correlation_id);
    
//     if(response.api_version < 5 || response.api_version > 11) {
//         std::cerr << "Invalid API version: " << response.api_version << std::endl;
//         response.api_version = 35; // Set to 35 for invalid versions
//     }
    
//     // Prepare response (convert to network byte order)
//     response.message_size = htonl(0);
//     uint16_t api_version_net = htons(response.api_version);  //api_version is 2 bytes
//     uint32_t correlation_id_net = htonl(response.correlation_id);
    
//     // Send response
//     // partial write
//     write(client_fd, &response.message_size, sizeof(response.message_size));
//     write(client_fd, &correlation_id_net, sizeof(correlation_id_net));
//     write(client_fd, &api_version_net, sizeof(api_version_net));
// }
class KafkaBuilder {
public:
    KafkaBuilder() {
        api_format.push_back(Fetch_api_format);
        api_format.push_back(ApiVersion_api_format);
        api_format.push_back(DescribeTopicPartitions_api_format);
    }

    ~KafkaBuilder() = default;

    void getResponseBuffer(const char* buffer) {
        getKafkaHeader(buffer);
        getKafkaBody(buffer);
        getKafkaMessage();
    }

    void getKafkaHeader(const char* buffer) {
        // correlation_id 在 request buffer 中偏移 8 个字节（4字节）
        memcpy(respondBuffer + 4, buffer + 8, sizeof(correlation_id));
        header_size = 8; // 4 字节长度 + 4 字节 correlation_id
    }

    void getKafkaBody(const char* buffer) {
        int16_t request_api_key;
        memcpy(&request_api_key, buffer + 4, sizeof(request_api_key));

        body_size = 0;

        // 写入 error_code（2字节）
        memcpy(respondBuffer + header_size + body_size, &error_code, sizeof(error_code));
        body_size += sizeof(error_code);

        // 写入 array_len（1字节）
        array_len = static_cast<int8_t>(api_format.size()) + 1;
        memcpy(respondBuffer + header_size + body_size, &array_len, sizeof(array_len));
        body_size += sizeof(array_len);

        // 写入每个 API version 格式
        for (int i = 0; i < api_format.size(); ++i) {
            int16_t api_key = htons(api_format[i][0]);
            int16_t min_api_version = htons(api_format[i][1]);
            int16_t max_api_version = htons(api_format[i][2]);

            // api_key (2字节)
            memcpy(respondBuffer + header_size + body_size, &api_key, sizeof(api_key));
            body_size += sizeof(api_key);

            // min_api_version (2字节)
            memcpy(respondBuffer + header_size + body_size, &min_api_version, sizeof(min_api_version));
            body_size += sizeof(min_api_version);

            // max_api_version (2字节)
            memcpy(respondBuffer + header_size + body_size, &max_api_version, sizeof(max_api_version));
            body_size += sizeof(max_api_version);

            // tag_buffer (1字节)
            memcpy(respondBuffer + header_size + body_size, &tag_buffer, sizeof(tag_buffer));
            body_size += sizeof(tag_buffer);
        }

        // throttle_time (4字节)
        memcpy(respondBuffer + header_size + body_size, &throttle_time, sizeof(throttle_time));
        body_size += sizeof(throttle_time);

        // tag_buffer (1字节)
        memcpy(respondBuffer + header_size + body_size, &tag_buffer, sizeof(tag_buffer));
        body_size += sizeof(tag_buffer);
    }

    void getKafkaMessage() {
        // 写入消息总长度：header + body（前 4 字节）
        int32_t total_size = htonl(header_size + body_size - 4);  // 不包含消息长度字段自身
        memcpy(respondBuffer, &total_size, sizeof(total_size));
    }

    // 可公开访问响应缓冲区和大小
    const char* getBuffer() const { return respondBuffer; }
    size_t getBufferSize() const { return header_size + body_size; }

private:
    vector<vector<int16_t>> api_format;
    vector<int16_t> Fetch_api_format{1, 0 , 11};
    vector<int16_t> ApiVersion_api_format{0x12, 0, 4};
    vector<int16_t> DescribeTopicPartitions_api_format{0x4b, 0, 0};

    char respondBuffer[1024]{};

    int32_t correlation_id{};
    int16_t error_code = htons(0);
    int8_t array_len{};
    int8_t tag_buffer = 0;
    int32_t throttle_time = htonl(0);

    int32_t header_size{};
    int32_t body_size{};
};
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
    // kafka_message response{};
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
    // parse_api_version(buffer, response, client_fd);.
    KafkaBuilder kafka_builder;
    kafka_builder.getResponseBuffer(buffer);
    send(client_fd, kafka_builder.getBuffer(), kafka_builder.getBufferSize(), 0);

    close(client_fd);
    close(server_fd);
    return 0;
}