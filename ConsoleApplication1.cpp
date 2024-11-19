#include <iostream>
#include <fstream>
#include <vector>
#include <algorithm>
#include <thread>
#include <mutex>
#include <queue>
#include <string>
#include <iterator>
#include <functional>
#include <sstream>
#include <filesystem>

// Для удобного доступа к файловым операциям
namespace fs = std::filesystem;

// Размер чанка в байтах
const size_t CHUNK_SIZE = 100 * 1024 * 1024; 

std::mutex mtx; // Для защиты очереди файлов

// Функция для сортировки одного чанка
void sortChunk(const std::string& inputFile, const std::string& outputFile) {
    std::ifstream in(inputFile, std::ios::binary);
    std::ofstream out(outputFile, std::ios::binary);

    // Читаем данные из файла
    std::vector<int> numbers((std::istream_iterator<int>(in)), std::istream_iterator<int>());
    in.close();

    // Сортируем данные в памяти
    std::sort(numbers.begin(), numbers.end());

    // Записываем отсортированные данные в новый файл
    for (int num : numbers) {
        out << num << "\n";
    }
    out.close();
}

// Многопоточная сортировка чанков
void sortChunksMultithreaded(const std::vector<std::string>& chunkFiles, std::vector<std::string>& sortedChunkFiles) {
    std::vector<std::thread> threads;
    for (const auto& chunkFile : chunkFiles) {
        std::string sortedFile = chunkFile + ".sorted";
        sortedChunkFiles.push_back(sortedFile);
        threads.emplace_back(sortChunk, chunkFile, sortedFile);
    }

    // Ждем завершения всех потоков
    for (auto& th : threads) {
        th.join();
    }
}

// Функция для слияния двух файлов
void mergeFiles(const std::string& file1, const std::string& file2, const std::string& outputFile) {
    std::ifstream in1(file1, std::ios::binary);
    std::ifstream in2(file2, std::ios::binary);
    std::ofstream out(outputFile, std::ios::binary);

    int num1, num2;
    bool hasNum1 = (in1 >> num1);
    bool hasNum2 = (in2 >> num2);

    while (hasNum1 && hasNum2) {
        if (num1 < num2) {
            out << num1 << "\n";
            hasNum1 = (in1 >> num1);
        }
        else {
            out << num2 << "\n";
            hasNum2 = (in2 >> num2);
        }
    }

    while (hasNum1) {
        out << num1 << "\n";
        hasNum1 = (in1 >> num1);
    }

    while (hasNum2) {
        out << num2 << "\n";
        hasNum2 = (in2 >> num2);
    }

    in1.close();
    in2.close();
    out.close();
}

// Многопутевое слияние файлов
void mergeChunks(const std::vector<std::string>& sortedChunkFiles, const std::string& outputFile) {
    std::priority_queue<std::pair<int, std::ifstream*>, std::vector<std::pair<int, std::ifstream*>>, std::greater<>> minHeap;
    std::vector<std::ifstream> chunkStreams;

    // Открываем все файлы и загружаем первые элементы
    for (const auto& sortedFile : sortedChunkFiles) {
        chunkStreams.emplace_back(sortedFile, std::ios::binary);
        int num;
        if (chunkStreams.back() >> num) {
            minHeap.emplace(num, &chunkStreams.back());
        }
    }

    std::ofstream out(outputFile, std::ios::binary);

    // Обрабатываем кучу
    while (!minHeap.empty()) {
        auto [num, fileStream] = minHeap.top();
        minHeap.pop();

        out << num << "\n";

        if ((*fileStream) >> num) {
            minHeap.emplace(num, fileStream);
        }
    }

    out.close();
    for (auto& stream : chunkStreams) {
        stream.close();
    }
}

int main() {
    std::string inputFile = "numbers.txt";
    std::string outputFile = "sorted_numbers.txt";

    std::vector<std::string> chunkFiles;
    std::vector<std::string> sortedChunkFiles;

    // Шаг 1: Чтение и разбиение на чанки
    {
        std::ifstream in(inputFile, std::ios::binary);
        size_t chunkIndex = 0;
        while (!in.eof()) {
            std::vector<int> buffer;
            buffer.reserve(CHUNK_SIZE / sizeof(int));
            int num;
            while (buffer.size() < CHUNK_SIZE / sizeof(int) && in >> num) {
                buffer.push_back(num);
            }

            if (!buffer.empty()) {
                std::string chunkFile = "chunk_" + std::to_string(chunkIndex++) + ".txt";
                std::ofstream out(chunkFile, std::ios::binary);
                for (int n : buffer) {
                    out << n << "\n";
                }
                out.close();
                chunkFiles.push_back(chunkFile);
            }
        }
    }

    // Шаг 2: Многопоточная сортировка чанков
    sortChunksMultithreaded(chunkFiles, sortedChunkFiles);

    // Шаг 3: Слияние чанков в итоговый файл
    mergeChunks(sortedChunkFiles, outputFile);

    // Удаление временных файлов
    for (const auto& file : chunkFiles) fs::remove(file);
    for (const auto& file : sortedChunkFiles) fs::remove(file);

    std::cout << "Сортировка завершена. Результат сохранен в " << outputFile << "\n";
    return 0;
}
