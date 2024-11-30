#include <iostream>
#include <fstream>
#include <vector>
#include <algorithm>
#include <thread>
#include <mutex>
#include <string>
#include <sstream>
#include <queue>
#include <functional>

// Ограничение памяти (1 ГБ)
const size_t MEMORY_LIMIT = 1L * 1024 * 1024 * 1024; // 1 GB
const size_t CHUNK_LINES = MEMORY_LIMIT / sizeof(int); // Количество строк в одном чанке

std::mutex mtx; // Для синхронизации потоков

// Функция сортировки куска данных
void sortChunk(std::vector<int>& data) {
    std::sort(data.begin(), data.end());
}

// Функция чтения, сортировки и записи кусочка файла
void sortFileChunk(const std::string& inputFile, const std::string& outputFile, size_t chunkStart, size_t chunkSize) {
    std::ifstream in(inputFile);
    std::ofstream out(outputFile, std::ios::in | std::ios::out);

    if (!in.is_open() || !out.is_open()) {
        std::cerr << "Ошибка открытия файла: " << inputFile << " или " << outputFile << "\n";
        return;
    }

    // Пропускаем строки до начала чанка
    in.seekg(0, std::ios::beg);
    size_t currentLine = 0;
    std::string line;

    while (currentLine < chunkStart && std::getline(in, line)) {
        ++currentLine;
    }

    // Читаем данные в память
    std::vector<int> buffer;
    while (currentLine < chunkStart + chunkSize && std::getline(in, line)) {
        buffer.push_back(std::stoi(line));
        ++currentLine;
    }

    // Сортируем данные
    sortChunk(buffer);

    // Позиционируемся на нужное место в выходном файле
    std::stringstream sortedData;
    for (const int num : buffer) {
        sortedData << num << "\n";
    }

    std::lock_guard<std::mutex> lock(mtx);
    out.seekp(0, std::ios::end);
    out << sortedData.str();

    in.close();
    out.close();
}

// Многопоточная сортировка всего файла
void sortFileMultithreaded(const std::string& inputFile, const std::string& tempFile, size_t totalLines) {
    size_t totalChunks = (totalLines + CHUNK_LINES - 1) / CHUNK_LINES; // Округление вверх

    std::vector<std::thread> threads;

    // Разделяем работу на чанки
    for (size_t chunkIndex = 0; chunkIndex < totalChunks; ++chunkIndex) {
        size_t chunkStart = chunkIndex * CHUNK_LINES; // Начало чанка
        size_t chunkSize = std::min(CHUNK_LINES, totalLines - chunkStart); // Размер чанка

        threads.emplace_back(sortFileChunk, inputFile, tempFile, chunkStart, chunkSize);
    }

    // Ждем завершения всех потоков
    for (auto& thread : threads) {
        thread.join();
    }
}

// Функция для многопоточного слияния частей
void mergeSortedChunks(const std::string& tempFile, const std::string& outputFile, size_t totalLines, size_t chunkSize) {
    size_t totalChunks = (totalLines + chunkSize - 1) / chunkSize; // Округление вверх

    std::ifstream in(tempFile);
    std::ofstream out(outputFile);

    if (!in.is_open() || !out.is_open()) {
        std::cerr << "Ошибка открытия файла для слияния: " << tempFile << " или " << outputFile << "\n";
        return;
    }

    // Используем минимальную кучу для слияния
    auto cmp = [](const std::pair<int, size_t>& a, const std::pair<int, size_t>& b) {
        return a.first > b.first; // Для min-heap
    };

    std::priority_queue<std::pair<int, size_t>, std::vector<std::pair<int, size_t>>, decltype(cmp)> minHeap(cmp);

    std::vector<std::ifstream> chunkStreams(totalChunks);

    for (size_t i = 0; i < totalChunks; ++i) {
        size_t start = i * chunkSize;
        size_t size = std::min(chunkSize, totalLines - start);

        chunkStreams[i].open(tempFile);
        for (size_t j = 0; j < start; ++j) {
            std::string line;
            std::getline(chunkStreams[i], line); // Пропускаем строки
        }

        int num;
        if (chunkStreams[i] >> num) {
            minHeap.emplace(num, i);
        }
    }

    while (!minHeap.empty()) {
        auto [num, chunkIndex] = minHeap.top();
        minHeap.pop();

        out << num << "\n";

        int nextNum;
        if (chunkStreams[chunkIndex] >> nextNum) {
            minHeap.emplace(nextNum, chunkIndex);
        }
    }

    for (auto& stream : chunkStreams) {
        if (stream.is_open()) stream.close();
    }

    in.close();
    out.close();
}

// Главная функция
int main() {
    std::string inputFile = "numbers.txt";       // Входной файл с числами
    std::string tempFile = "temp_sorted.txt";    // Временный файл для промежуточной сортировки
    std::string outputFile = "sorted_numbers.txt"; // Выходной файл с отсортированными числами

    // Подсчитываем количество строк в входном файле
    size_t totalLines = 0;
    {
        std::ifstream in(inputFile);
        std::string line;
        while (std::getline(in, line)) {
            ++totalLines;
        }
        in.close();
    }

    // Создаем временный файл
    {
        std::ofstream temp(tempFile);
        temp.close();
    }

    // Многопоточная сортировка всего файла
    sortFileMultithreaded(inputFile, tempFile, totalLines);

    // Многопоточное слияние
    mergeSortedChunks(tempFile, outputFile, totalLines, CHUNK_LINES);

    // Удаляем временный файл
    std::remove(tempFile.c_str());

    std::cout << "Файл успешно отсортирован. Результат сохранен в " << outputFile << "\n";
    return 0;
}