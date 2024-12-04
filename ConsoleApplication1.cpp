#include <iostream>
#include <fstream>
#include <vector>
#include <algorithm>
#include <thread>
#include <mutex>
#include <queue>
#include <string>
#include <functional>
#include <cmath>

// Ограничение памяти для одного чанка (1 ГБ)
const size_t MEMORY_LIMIT = 1L * 1024 * 1024 * 1024; // Размер памяти для одного чанка в байтах (1 ГБ)

// Максимальное количество чисел, которые можно загрузить в память для сортировки
const size_t MAX_NUMBERS_IN_MEMORY = MEMORY_LIMIT / sizeof(int); // Максимальное количество чисел, которые помещаются в память

// Мьютекс для синхронизации потоков
std::mutex mtx; // Используется для предотвращения одновременной записи в файл несколькими потоками

// Функция сортировки данных в одном чанке
void sortChunk(std::vector<int>& data) {
    std::sort(data.begin(), data.end()); // Сортировка чисел в памяти с использованием стандартного алгоритма
}

// Функция сортировки чанков в многопоточном режиме
void sortFileChunk(const std::string& inputFile, const std::string& tempFile, size_t chunkStart, size_t chunkSize) {
    std::ifstream in(inputFile, std::ios::binary); // Открываем входной файл в бинарном режиме
    std::ofstream out(tempFile, std::ios::binary | std::ios::app); // Открываем временный файл в режиме добавления

    if (!in.is_open() || !out.is_open()) {
        // Проверяем, открылись ли файлы
        std::cerr << "Ошибка открытия файла: " << inputFile << " или " << tempFile << "\n";
        return; // Завершаем функцию в случае ошибки
    }

    // Перемещаем указатель чтения на начало чанка
    in.seekg(chunkStart);

    // Читаем числа из файла
    std::vector<int> buffer; // Буфер для хранения чисел из текущего чанка
    size_t numbersRead = 0; // Количество прочитанных чисел
    while (numbersRead < chunkSize && in) {
        int num;
        in >> num; // Считываем число
        buffer.push_back(num); // Добавляем его в буфер
        numbersRead++;
    }

    // Сортируем данные в памяти
    sortChunk(buffer);

    // Блокируем мьютекс для записи в файл
    {
        std::lock_guard<std::mutex> lock(mtx); // Гарантируем, что запись в файл будет происходить только одним потоком
        for (const int num : buffer) {
            out << num << "\n"; // Записываем отсортированные числа в файл
        }
    }

    in.close(); // Закрываем входной файл
    out.close(); // Закрываем временный файл
}

// Функция для многопоточной сортировки всего файла
void sortFileMultithreaded(const std::string& inputFile, const std::string& tempFile, size_t fileSize) {
    size_t totalChunks = std::ceil(static_cast<double>(fileSize) / MEMORY_LIMIT); // Рассчитываем количество чанков

    std::vector<std::thread> threads; // Вектор потоков для параллельной обработки

    for (size_t chunkIndex = 0; chunkIndex < totalChunks; ++chunkIndex) {
        size_t chunkStart = chunkIndex * MEMORY_LIMIT; // Начало текущего чанка
        size_t chunkEnd = std::min(fileSize, (chunkIndex + 1) * MEMORY_LIMIT); // Конец текущего чанка
        size_t chunkSize = (chunkEnd - chunkStart) / sizeof(int); // Количество чисел в чанке

        // Создаем поток для сортировки текущего чанка
        threads.emplace_back(sortFileChunk, inputFile, tempFile, chunkStart, chunkSize);
    }

    // Дожидаемся завершения всех потоков
    for (auto& thread : threads) {
        thread.join();
    }
}

// Функция слияния отсортированных чанков
void mergeSortedChunks(const std::string& tempFile, const std::string& outputFile, size_t totalChunks, size_t chunkSize) {
    std::ifstream in(tempFile); // Открываем временный файл
    std::ofstream out(outputFile); // Открываем выходной файл

    if (!in.is_open() || !out.is_open()) {
        // Проверяем, открылись ли файлы
        std::cerr << "Ошибка открытия файла: " << tempFile << " или " << outputFile << "\n";
        return;
    }

    // Определяем компаратор для создания мин-кучи
    auto cmp = [](const std::pair<int, size_t>& a, const std::pair<int, size_t>& b) {
        return a.first > b.first;  // Порядок в куче: минимальный элемент в вершине
    };

    // Создаем мин-кучу
    std::priority_queue<std::pair<int, size_t>, std::vector<std::pair<int, size_t>>, decltype(cmp)> minHeap(cmp);

    // Открываем потоки для каждого чанка
    std::vector<std::ifstream> chunkStreams(totalChunks);
    for (size_t i = 0; i < totalChunks; ++i) {
        chunkStreams[i].open(tempFile); // Открываем временный файл для каждого чанка
        chunkStreams[i].seekg(i * chunkSize * sizeof(int)); // Перемещаемся к началу чанка

        int num;
        if (chunkStreams[i] >> num) {
            // Добавляем первый элемент каждого чанка в мин-кучу
            minHeap.emplace(num, i);
        }
    }

    while (!minHeap.empty()) {
        // Извлекаем минимальный элемент
        auto [num, chunkIndex] = minHeap.top();
        minHeap.pop();

        out << num << "\n"; // Записываем минимальный элемент в выходной файл

        int nextNum;
        if (chunkStreams[chunkIndex] >> nextNum) {
            // Если в чанке есть еще элементы, добавляем следующий элемент в мин-кучу
            minHeap.emplace(nextNum, chunkIndex);
        }
    }

    // Закрываем все потоки
    for (auto& stream : chunkStreams) {
        if (stream.is_open()) stream.close();
    }

    in.close();
    out.close();
}

// Главная функция
int main() {
    // Имена файлов
    std::string inputFile = "numbers.txt";       // Входной файл с числами
    std::string tempFile = "temp_sorted.txt";    // Временный файл для промежуточной сортировки
    std::string outputFile = "sorted_numbers.txt"; // Выходной файл с отсортированными числами

    // Определяем размер входного файла
    std::ifstream in(inputFile, std::ios::binary | std::ios::ate);
    if (!in.is_open()) {
        std::cerr << "Ошибка открытия входного файла: " << inputFile << "\n";
        return 1;
    }
    size_t fileSize = in.tellg(); // Получаем размер файла
    in.close();

    // Очищаем временный файл
    std::ofstream temp(tempFile, std::ios::trunc);
    temp.close();

    // Рассчитываем общее количество чанков
    size_t totalChunks = std::ceil(static_cast<double>(fileSize) / MEMORY_LIMIT);

    // Выполняем сортировку файла
    sortFileMultithreaded(inputFile, tempFile, fileSize);

    // Выполняем слияние отсортированных чанков
    mergeSortedChunks(tempFile, outputFile, totalChunks, MAX_NUMBERS_IN_MEMORY);

    // Удаляем временный файл
    std::remove(tempFile.c_str());

    std::cout << "Файл успешно отсортирован. Результат сохранен в " << outputFile << "\n";
    return 0; // Возвращаем успешный код завершения
}