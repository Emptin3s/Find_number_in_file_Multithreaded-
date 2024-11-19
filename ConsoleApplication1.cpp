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
#include <memory> // Для std::unique_ptr

// Для удобного доступа к файловым операциям
namespace fs = std::filesystem;

// Размер чанка в байтах
const size_t CHUNK_SIZE = 100 * 1024 * 1024; // 100 MB

std::mutex mtx; // Для защиты общего доступа к ресурсам (если нужно в будущем)

// Функция для сортировки одного чанка
void sortChunk(const std::string& inputFile, const std::string& outputFile) {
    // Открытие входного файла
    std::ifstream in(inputFile);
    // Открытие выходного файла
    std::ofstream out(outputFile);

    // Проверяем успешность открытия файлов
    if (!in.is_open()) {
        std::cerr << "Ошибка: Не удалось открыть файл " << inputFile << "\n";
        return;
    }
    if (!out.is_open()) {
        std::cerr << "Ошибка: Не удалось создать файл " << outputFile << "\n";
        return;
    }

    // Чтение данных из файла в вектор
    std::vector<int> numbers((std::istream_iterator<int>(in)), std::istream_iterator<int>());
    in.close();

    // Сортировка данных в памяти
    std::sort(numbers.begin(), numbers.end());

    // Запись отсортированных данных в выходной файл
    for (int num : numbers) {
        out << num << "\n";
    }
    out.close();
}

// Многопоточная сортировка чанков
void sortChunksMultithreaded(const std::vector<std::string>& chunkFiles, std::vector<std::string>& sortedChunkFiles) {
    std::vector<std::thread> threads; // Контейнер для управления потоками

    // Создаём поток для каждого чанка
    for (const auto& chunkFile : chunkFiles) {
        // Создаём имя для выходного файла
        std::string sortedFile = chunkFile + ".sorted";
        sortedChunkFiles.push_back(sortedFile);

        // Запускаем поток для сортировки
        threads.emplace_back(sortChunk, chunkFile, sortedFile);
    }

    // Ждём завершения всех потоков
    for (auto& th : threads) {
        th.join();
    }
}

// Функция для слияния отсортированных файлов
void mergeChunks(const std::vector<std::string>& sortedChunkFiles, const std::string& outputFile) {
    // Минимальная куча для поддержки минимального элемента среди файлов
    std::priority_queue<
        std::pair<int, std::ifstream*>,
        std::vector<std::pair<int, std::ifstream*>>,
        std::greater<>
    > minHeap;

    // Используем уникальные указатели для автоматического управления потоками
    std::vector<std::unique_ptr<std::ifstream>> chunkStreams;

    // Открываем все отсортированные файлы и добавляем первые элементы в кучу
    for (const auto& sortedFile : sortedChunkFiles) {
        auto stream = std::make_unique<std::ifstream>(sortedFile);
        if (!stream->is_open()) {
            std::cerr << "Ошибка: Не удалось открыть файл " << sortedFile << "\n";
            continue;
        }

        int num;
        // Читаем первый элемент файла, если он не пуст
        if (*stream >> num) {
            minHeap.emplace(num, stream.get()); // Добавляем в кучу
            chunkStreams.push_back(std::move(stream)); // Сохраняем поток
        }
        else {
            std::cerr << "Ошибка: Пустой файл " << sortedFile << "\n";
        }
    }

    // Открываем выходной файл
    std::ofstream out(outputFile);
    if (!out.is_open()) {
        std::cerr << "Ошибка: Не удалось открыть выходной файл " << outputFile << "\n";
        return;
    }

    // Постепенно сливаем элементы из кучи в выходной файл
    while (!minHeap.empty()) {
        auto [num, fileStream] = minHeap.top();
        minHeap.pop();

        // Записываем минимальный элемент в выходной файл
        out << num << "\n";

        // Считываем следующий элемент из соответствующего файла
        if (*fileStream >> num) {
            minHeap.emplace(num, fileStream); // Возвращаем файл в кучу с обновлённым значением
        }
    }

    out.close();
}

// Главная функция программы
int main() {
    std::string inputFile = "numbers.txt"; // Входной файл с числами
    std::string outputFile = "sorted_numbers.txt"; // Итоговый файл с отсортированными числами

    std::vector<std::string> chunkFiles; // Список файлов-чанков
    std::vector<std::string> sortedChunkFiles; // Список отсортированных файлов-чанков

    // Шаг 1: Чтение исходного файла и разбиение на чанки
    {
        std::ifstream in(inputFile);
        if (!in.is_open()) {
            std::cerr << "Ошибка: Не удалось открыть входной файл " << inputFile << "\n";
            return -1;
        }

        size_t chunkIndex = 0;
        while (!in.eof()) {
            std::vector<int> buffer;
            buffer.reserve(CHUNK_SIZE / sizeof(int)); // Резервируем память для буфера
            int num;

            // Читаем данные до заполнения буфера или конца файла
            while (buffer.size() < CHUNK_SIZE / sizeof(int) && in >> num) {
                buffer.push_back(num);
            }

            // Если буфер не пуст, записываем его в файл
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
                chunkFiles.push_back(chunkFile); // Добавляем файл в список чанков
            }
        }
        in.close();
    }

    // Шаг 2: Многопоточная сортировка чанков
    sortChunksMultithreaded(chunkFiles, sortedChunkFiles);

    // Шаг 3: Слияние отсортированных чанков в итоговый файл
    mergeChunks(sortedChunkFiles, outputFile);

    // Шаг 4: Удаление временных файлов
    for (const auto& file : chunkFiles) fs::remove(file);
    for (const auto& file : sortedChunkFiles) fs::remove(file);

    std::cout << "Сортировка завершена. Результат сохранен в " << outputFile << "\n";
    return 0;
}
