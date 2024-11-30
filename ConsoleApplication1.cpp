#include <iostream>
#include <fstream>
#include <vector>
#include <algorithm>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <functional>

// Ограничение на использование памяти (1 GB)
const size_t MEMORY_LIMIT = 1L * 1024 * 1024 * 1024; // 1 GB
const size_t CHUNK_ELEMENTS = MEMORY_LIMIT / sizeof(int); // Количество элементов в одном куске

std::mutex mtx; // Для синхронизации потоков
std::condition_variable cv; // Для координации между потоками

// Функция сортировки куска данных
void sortChunk(std::vector<int>& data) {
    std::sort(data.begin(), data.end());
}

// Функция чтения, сортировки и записи кусочка файла
void sortFileChunk(const std::string& fileName, size_t chunkStart, size_t chunkSize) {
    std::ifstream in(fileName, std::ios::binary);
    std::ofstream out(fileName, std::ios::binary | std::ios::in | std::ios::out);

    if (!in.is_open() || !out.is_open()) {
        std::cerr << "Ошибка открытия файла: " << fileName << "\n";
        return;
    }

    // Позиционируемся на начало чанка
    in.seekg(chunkStart * sizeof(int));
    out.seekp(chunkStart * sizeof(int));

    // Читаем данные в память
    std::vector<int> buffer(chunkSize);
    in.read(reinterpret_cast<char*>(&buffer[0]), chunkSize * sizeof(int));

    size_t elementsRead = in.gcount() / sizeof(int);
    buffer.resize(elementsRead);

    // Сортируем данные
    sortChunk(buffer);

    // Записываем отсортированные данные обратно
    out.write(reinterpret_cast<const char*>(&buffer[0]), elementsRead * sizeof(int));

    in.close();
    out.close();
}

// Многопоточная сортировка всего файла
void sortFileMultithreaded(const std::string& fileName, size_t fileSize) {
    size_t totalElements = fileSize / sizeof(int);
    size_t totalChunks = (totalElements + CHUNK_ELEMENTS - 1) / CHUNK_ELEMENTS; // Округление вверх

    std::vector<std::thread> threads;

    // Разделяем работу на чанки
    for (size_t chunkIndex = 0; chunkIndex < totalChunks; ++chunkIndex) {
        size_t chunkStart = chunkIndex * CHUNK_ELEMENTS; // Начало чанка
        size_t chunkSize = std::min(CHUNK_ELEMENTS, totalElements - chunkStart); // Размер чанка

        threads.emplace_back(sortFileChunk, fileName, chunkStart, chunkSize);
    }

    // Ждем завершения всех потоков
    for (auto& thread : threads) {
        thread.join();
    }
}

// Функция для многопоточного слияния частей
void mergeSortedChunks(const std::string& fileName, size_t fileSize, size_t chunkSize) {
    size_t totalElements = fileSize / sizeof(int);
    size_t totalChunks = (totalElements + chunkSize - 1) / chunkSize; // Округление вверх

    std::ifstream in(fileName, std::ios::binary);
    std::ofstream out(fileName + ".sorted", std::ios::binary);

    if (!in.is_open() || !out.is_open()) {
        std::cerr << "Ошибка открытия файла для слияния: " << fileName << "\n";
        return;
    }

    // Используем минимальную кучу для многопоточного слияния
    auto cmp = [](const std::pair<int, size_t>& a, const std::pair<int, size_t>& b) {
        return a.first > b.first; // Для min-heap
    };

    std::priority_queue<std::pair<int, size_t>, std::vector<std::pair<int, size_t>>, decltype(cmp)> minHeap(cmp);

    std::vector<std::ifstream> chunkStreams(totalChunks);

    for (size_t i = 0; i < totalChunks; ++i) {
        size_t start = i * chunkSize;
        size_t size = std::min(chunkSize, totalElements - start);

        chunkStreams[i].open(fileName, std::ios::binary);
        chunkStreams[i].seekg(start * sizeof(int));

        int num;
        if (chunkStreams[i].read(reinterpret_cast<char*>(&num), sizeof(int))) {
            minHeap.emplace(num, i);
        }
    }

    while (!minHeap.empty()) {
        auto [num, chunkIndex] = minHeap.top();
        minHeap.pop();

        out.write(reinterpret_cast<const char*>(&num), sizeof(int));

        int nextNum;
        if (chunkStreams[chunkIndex].read(reinterpret_cast<char*>(&nextNum), sizeof(int))) {
            minHeap.emplace(nextNum, chunkIndex);
        }
    }

    in.close();
    out.close();
}

// Главная функция
int main() {
    std::string inputFile = "numbers.bin"; // Входной файл с числами

    // Получаем размер входного файла
    std::ifstream in(inputFile, std::ios::binary | std::ios::ate);
    size_t fileSize = in.tellg();
    in.close();

    // Многопоточная сортировка всего файла
    sortFileMultithreaded(inputFile, fileSize);

    // Многопоточное слияние
    mergeSortedChunks(inputFile, fileSize, CHUNK_ELEMENTS);

    std::cout << "Файл успешно отсортирован.\n";
    return 0;
}
