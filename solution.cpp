

/*
 * Copyright Ram Nindra (ramnindra@yahoo.com)
 */

#include <iostream>
#include <fcntl.h>
#include <pthread.h>
#include <sys/mman.h> // for mmap()
#include <sys/stat.h> // for fstat
#include <strings.h>
#include <unistd.h>
#include <iomanip>
#include <chrono>
#include <thread>
#include <unordered_map>

#define FILE_1_NAME "file1"
#define FILE_2_NAME "file2"

#define THREAD_COUNT 4

typedef struct {
    char *chunk_start;
    char *chunk_end;
    int chunk_id;
    int number_of_lines;
    int line_number_start;
    int line_number_end;
} chunk_thread_data;
/*
    // Starting time for the clock
    auto start = high_resolution_clock::now();

    // Ending time for the clock
    auto stop = high_resolution_clock::now();

    auto duration = duration_cast<microseconds>(stop - start);
*/

static void* chunk_worker_thread(void *thread_data) 
{
    int word_count = 0;
    char file_1_path[256] = "";
    char file_2_path[256] = "";
    char word[256] = "";
    chunk_thread_data *cdata = (chunk_thread_data *)thread_data;
    sprintf(file_1_path, "%s_%d.txt", FILE_1_NAME, cdata->chunk_id);
    sprintf(file_2_path, "%s_%d.txt", FILE_2_NAME, cdata->chunk_id);
    std::unordered_map<std::string, int> mymap; //good to have default size preallocated : it is a bottleneck
    FILE* file_1_fp = fopen(file_1_path, "w+");
    if (file_1_fp == NULL) {
        return NULL;
    }
    FILE* file_2_fp = fopen(file_2_path, "w+");
    if (file_2_fp == NULL) {
        return NULL;
    }
    char* ptr = cdata->chunk_start;
    int index = 0;
    while(ptr <= cdata->chunk_end) {
        bool space = isspace(*ptr);
        if (space == true) {
            if (index != 0) {
                word_count++;
                word[index] = '\0';
                mymap[word]++;
                index = 0;
            }
            //std::cout << "[" << word << "]" << std::endl;
        } else {
            word[index++] = *ptr;
        }
        if (*ptr == '\n') {
            index = 0;
            cdata->number_of_lines++;
            fprintf(file_1_fp, "%d\n", word_count);
            word_count = 0;
        }
        ptr++;
    }
    fclose(file_1_fp);
    for (auto i : mymap) {
        fprintf(file_2_fp, "%s %d\n", i.first.c_str(), i.second);
    }
    fclose(file_2_fp);
    //std::cout << "start = " << static_cast<void*>(cdata->chunk_start) << " ";
    //std::cout << "end = " << static_cast<void*>(cdata->chunk_end) << " ";
    return NULL;
}

bool process_file(char* path)
{
    pthread_t threads[THREAD_COUNT];
    chunk_thread_data thread_data[THREAD_COUNT];

    bzero(thread_data, sizeof(chunk_thread_data)*THREAD_COUNT);

    int fd = open(path, O_RDWR);
    if (fd == -1) {
        return false;
    }
    struct stat sb;
    if (fstat(fd, &sb) == -1) {
        return false;
    }
    uint64_t total_bytes = sb.st_size;
    char *mmap_addr_start = (char*)mmap(NULL, total_bytes, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    char *mmap_addr_end = mmap_addr_start + total_bytes;

    //std::cout << "total_bytes " << total_bytes << std::endl;

    char *chunks[THREAD_COUNT];
    for (int i = 0; i < THREAD_COUNT; i++) {
        chunks[i] = mmap_addr_start + (total_bytes / THREAD_COUNT) * i; //Potential bug what if total_bytes not equally divisible
        if (i > 0) {
            while (*chunks[i] != '\n') {
                chunks[i]++;
            }
            chunks[i]++;
        }
        //std::cout << "Addr Chunk Thread " << i << " : " << static_cast<void*>(chunks[i]) << std::endl;
    }
    for (int i = 0; i < THREAD_COUNT; i++) {
        char *chunk_start = chunks[i];
        char *chunk_end = mmap_addr_end;
        if (i < THREAD_COUNT - 1) {
            chunk_end = chunks[i + 1];
        }
        thread_data[i].chunk_start = chunk_start;
        thread_data[i].chunk_end = chunk_end;
        thread_data[i].chunk_id = i;
        pthread_create(&threads[i], NULL, chunk_worker_thread, &thread_data[i]);
    }
    //Wait for worker threads to finish their jobs
    for (int i = 0; i < THREAD_COUNT; i++) {
        pthread_join(threads[i], NULL);
    }
    int total_lines = 0;
    for (int i = 0; i < THREAD_COUNT; i++) {
        if (i == 0) {
            thread_data[i].line_number_start = 1;
            thread_data[i].line_number_end = thread_data[i].number_of_lines;
        } else {
            thread_data[i].line_number_start = thread_data[i -1].line_number_end + 1;
            thread_data[i].line_number_end = thread_data[i].line_number_start + thread_data[i].number_of_lines;
        }
        total_lines += thread_data[i].number_of_lines;
        //std::cout << "Chunk Thread " << i << " : start = " << thread_data[i].line_number_start << 
          //           " end = " << thread_data[i].line_number_end << std::endl;
    }

    std::cout << "\nTotal Lines : " << total_lines << std::endl;
    close(fd);

    int line_num = 1;
    FILE* file1_fp = fopen("file1.txt", "w+");
    // Merge all files into one in order
    for (int i = 0; i < THREAD_COUNT; i++) {
        char file_1_path[256] = "";
        sprintf(file_1_path, "%s_%d.txt", FILE_1_NAME, i);
        FILE* file_1_fp = fopen(file_1_path, "r+");
        if (file_1_fp == NULL) {
            return NULL;
        }
        int wcount = 0;
        char* line = NULL;
        size_t len = 0;
        while ((getline(&line, &len, file_1_fp)) != -1) {
            // using fprintf() in all tests for consistency
            fprintf(file1_fp, "%d %s", line_num, line);
            line_num++;
        }
        //closing file pointer
        fclose(file_1_fp);
        if (line)
            free(line);
        //delele the file
        remove(file_1_path);
    }
    fclose(file1_fp);
    //munmap
    if (munmap(mmap_addr_start, total_bytes) == -1) {
        return false;
    }
    return true;
}

void displayTime(std::chrono::time_point<std::chrono::high_resolution_clock>& start)
{
    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    std::cout << "Start time: " << std::chrono::duration_cast<std::chrono::microseconds>(start.time_since_epoch()).count() << std::endl;
    std::cout << "End time: " << std::chrono::duration_cast<std::chrono::microseconds>(stop.time_since_epoch()).count() << std::endl;
    std::cout << "Time taken : " << duration.count() << " microseconds" << std::endl;
}

int main(int argc, char *argv[]) 
{
    // Starting time for the clock
    auto start = std::chrono::high_resolution_clock::now();
    if (argc != 2) 
    {
        std::cout << "Not correct number of argments passed\n";
        return -1;
    }
    process_file(argv[1]);
    // Ending time for the clock
    //displayTime(start);
    return 0;
} 
