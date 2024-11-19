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
const size_t CHUNK_SIZE = 100 * 1024 * 1024; // 100 MB

std::mutex mtx; // Для защиты очереди файлов

// Функция для сортировки одного чанка
void sortChunk(const std::string& inputFile, const std::string& outputFile) {
    std::ifstream in(inputFile);
    std::ofstream out(outputFile);

    // Проверка на успешное открытие файлов
    if (!in.is_open()) {
        std::cerr << "Ошибка: Не удалось открыть файл " << inputFile << "\n";
        return;
    }
    if (!out.is_open()) {
        std::cerr << "Ошибка: Не удалось создать файл " << outputFile << "\n";
        return;
    }

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
    std::ifstream in1(file1);
    std::ifstream in2(file2);
    std::ofstream out(outputFile);

    // Проверка на успешное открытие файлов
    if (!in1.is_open() || !in2.is_open()) {
        std::cerr << "Ошибка: Не удалось открыть файлы для слияния.\n";
        return;
    }

    int num1, num2;
    bool hasNum1 = static_cast<bool>(in1 >> num1); // Явное приведение к bool
    bool hasNum2 = static_cast<bool>(in2 >> num2); // Явное приведение к bool

    while (hasNum1 && hasNum2) {
        if (num1 < num2) {
            out << num1 << "\n";
            hasNum1 = static_cast<bool>(in1 >> num1); // Явное приведение
        }
        else {
            out << num2 << "\n";
            hasNum2 = static_cast<bool>(in2 >> num2); // Явное приведение
        }
    }

    while (hasNum1) {
        out << num1 << "\n";
        hasNum1 = static_cast<bool>(in1 >> num1); // Явное приведение
    }

    while (hasNum2) {
        out << num2 << "\n";
        hasNum2 = static_cast<bool>(in2 >> num2); // Явное приведение
    }

    in1.close();
    in2.close();
    out.close();
}

// Многопутевое слияние файлов
void mergeChunks(const std::vector<std::string>& sortedChunkFiles, const std::string& outputFile) {
    std::priority_queue<std::pair<int, std::ifstream*>, std::vector<std::pair<int, std::ifstream*>>, std::greater<>> minHeap;
    std::vector<std::ifstream*> chunkStreams;

    // Открываем все файлы и загружаем первые элементы
    for (const auto& sortedFile : sortedChunkFiles) {
        auto* stream = new std::ifstream(sortedFile);
        if (!stream->is_open()) {
            std::cerr << "Ошибка: Не удалось открыть файл " << sortedFile << "\n";
            delete stream;
            continue;
        }

        int num;
        if (*stream >> num) {
            minHeap.emplace(num, stream);
            chunkStreams.push_back(stream);
        }
        else {
            std::cerr << "Ошибка: Пустой файл " << sortedFile << "\n";
            delete stream;
        }
    }

    std::ofstream out(outputFile);
    if (!out.is_open()) {
        std::cerr << "Ошибка: Не удалось открыть выходной файл " << outputFile << "\n";
        return;
    }

    // Обрабатываем кучу
    while (!minHeap.empty()) {
        auto [num, fileStream] = minHeap.top();
        minHeap.pop();

        out << num << "\n";

        if (*fileStream >> num) {
            minHeap.emplace(num, fileStream);
        }
        else {
            delete fileStream; // Закрываем поток, если он исчерпан
        }
    }

    out.close();

    // Закрываем оставшиеся потоки
    for (auto* stream : chunkStreams) {
        if (stream->is_open()) stream->close();
        delete stream;
    }
}

int main() {
    std::string inputFile = "numbers.txt";
    std::string outputFile = "sorted_numbers.txt";

    std::vector<std::string> chunkFiles;
    std::vector<std::string> sortedChunkFiles;

    // Шаг 1: Чтение и разбиение на чанки
    {
        std::ifstream in(inputFile);
        if (!in.is_open()) {
            std::cerr << "Ошибка: Не удалось открыть входной файл " << inputFile << "\n";
            return -1;
        }

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
                std::ofstream out(chunkFile);
                if (!out.is_open()) {
                    std::cerr << "Ошибка: Не удалось создать файл " << chunkFile << "\n";
                    return -1;
                }

                for (int n : buffer) {
                    out << n << "\n";
                }
                out.close();
                chunkFiles.push_back(chunkFile);
            }
        }
        in.close();
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
