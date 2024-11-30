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

// Ограничение памяти для одного чанка (1 ГБ)
const size_t MEMORY_LIMIT = 1L * 1024 * 1024 * 1024; // 1 ГБ
// Количество строк в одном чанке, основываясь на размере данных
const size_t CHUNK_LINES = MEMORY_LIMIT / sizeof(int); // Размер чанка (в строках)

// Мьютекс для синхронизации потоков при записи в выходной файл
std::mutex mtx;

// Функция сортировки данных в одном чанке
void sortChunk(std::vector<int>& data) {
    std::sort(data.begin(), data.end()); // Стандартная сортировка
}

// Функция, которая читает кусочек данных из входного файла, сортирует его и записывает в выходной файл
void sortFileChunk(const std::string& inputFile, const std::string& outputFile, size_t chunkStart, size_t chunkSize) {
    std::ifstream in(inputFile);  // Открытие входного файла для чтения
    std::ofstream out(outputFile, std::ios::in | std::ios::out); // Открытие выходного файла для записи

    // Проверка, что файлы открылись корректно
    if (!in.is_open() || !out.is_open()) {
        std::cerr << "Ошибка открытия файла: " << inputFile << " или " << outputFile << "\n";
        return;
    }

    // Пропускаем строки до начала чанка
    in.seekg(0, std::ios::beg);  // Переход к началу файла
    size_t currentLine = 0;
    std::string line;

    // Пропускаем строки до начала текущего чанка
    while (currentLine < chunkStart && std::getline(in, line)) {
        ++currentLine;
    }

    // Чтение данных в память для сортировки
    std::vector<int> buffer;  // Вектор для хранения данных чанка
    while (currentLine < chunkStart + chunkSize && std::getline(in, line)) {
        buffer.push_back(std::stoi(line)); // Чтение числа и добавление в вектор
        ++currentLine;
    }

    // Сортировка данных в чанке
    sortChunk(buffer);

    // Позиционируемся на нужное место в выходном файле
    std::stringstream sortedData;
    for (const int num : buffer) {
        sortedData << num << "\n";  // Записываем отсортированные числа в строковый поток
    }

    // Блокировка мьютекса для безопасной записи в файл
    std::lock_guard<std::mutex> lock(mtx);
    out.seekp(0, std::ios::end);  // Переход в конец выходного файла
    out << sortedData.str();  // Записываем отсортированные данные

    // Закрытие файлов
    in.close();
    out.close();
}

// Функция многопоточной сортировки всего файла
void sortFileMultithreaded(const std::string& inputFile, const std::string& tempFile, size_t totalLines) {
    // Вычисление количества чанков, в которые нужно разделить файл
    size_t totalChunks = (totalLines + CHUNK_LINES - 1) / CHUNK_LINES; // Округление вверх

    std::vector<std::thread> threads;  // Вектор для потоков

    // Разделение работы на чанки и создание потоков для сортировки каждого чанка
    for (size_t chunkIndex = 0; chunkIndex < totalChunks; ++chunkIndex) {
        size_t chunkStart = chunkIndex * CHUNK_LINES;  // Начало текущего чанка
        size_t chunkSize = std::min(CHUNK_LINES, totalLines - chunkStart);  // Размер чанка (может быть меньше для последнего чанка)

        // Создание потока для сортировки текущего чанка
        threads.emplace_back(sortFileChunk, inputFile, tempFile, chunkStart, chunkSize);
    }

    // Ожидание завершения всех потоков
    for (auto& thread : threads) {
        thread.join();
    }
}

// Функция многопоточного слияния отсортированных частей
void mergeSortedChunks(const std::string& tempFile, const std::string& outputFile, size_t totalLines, size_t chunkSize) {
    // Вычисление количества чанков
    size_t totalChunks = (totalLines + chunkSize - 1) / chunkSize; // Округление вверх

    std::ifstream in(tempFile);  // Открытие временного файла для чтения
    std::ofstream out(outputFile);  // Открытие выходного файла для записи

    // Проверка открытия файлов
    if (!in.is_open() || !out.is_open()) {
        std::cerr << "Ошибка открытия файла для слияния: " << tempFile << " или " << outputFile << "\n";
        return;
    }

    // Определение компаратора для минимальной кучи (для слияния)
    auto cmp = [](const std::pair<int, size_t>& a, const std::pair<int, size_t>& b) {
        return a.first > b.first;  // Для min-heap
    };

    // Приоритетная очередь для слияния данных
    std::priority_queue<std::pair<int, size_t>, std::vector<std::pair<int, size_t>>, decltype(cmp)> minHeap(cmp);

    std::vector<std::ifstream> chunkStreams(totalChunks);  // Потоки для каждого чанка

    // Инициализация потоков для чтения каждого чанка
    for (size_t i = 0; i < totalChunks; ++i) {
        size_t start = i * chunkSize;  // Начало текущего чанка
        size_t size = std::min(chunkSize, totalLines - start);  // Размер чанка

        chunkStreams[i].open(tempFile);  // Открытие потока для текущего чанка
        for (size_t j = 0; j < start; ++j) {
            std::string line;
            std::getline(chunkStreams[i], line);  // Пропуск строк до начала чанка
        }

        int num;
        if (chunkStreams[i] >> num) {
            minHeap.emplace(num, i);  // Добавление первого элемента чанка в кучу
        }
    }

    // Слияние данных с использованием минимальной кучи
    while (!minHeap.empty()) {
        auto [num, chunkIndex] = minHeap.top();  // Извлекаем минимальное число из кучи
        minHeap.pop();

        out << num << "\n";  // Записываем число в выходной файл

        // Чтение следующего числа из того же чанка
        int nextNum;
        if (chunkStreams[chunkIndex] >> nextNum) {
            minHeap.emplace(nextNum, chunkIndex);  // Добавление нового элемента в кучу
        }
    }

    // Закрытие всех потоков
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
            ++totalLines;  // Подсчитываем количество строк
        }
        in.close();
    }

    // Создаем временный файл
    {
        std::ofstream temp(tempFile);
        temp.close();  // Закрытие файла сразу после его создания
    }

    // Многопоточная сортировка всего файла
    sortFileMultithreaded(inputFile, tempFile, totalLines);

    // Многопоточное слияние отсортированных частей
    mergeSortedChunks(tempFile, outputFile, totalLines, CHUNK_LINES);

    // Удаление временного файла
    std::remove(tempFile.c_str());

    std::cout << "Файл успешно отсортирован. Результат сохранен в " << outputFile << "\n";
    return 0;
}