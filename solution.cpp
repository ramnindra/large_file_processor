

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
#include <map>
#include <cstring>
#include <stdio.h>

#define FILE_1_NAME "file1"
#define FILE_2_NAME "file2"

//Total RAM is roughly among all threads
// in system with RAM 16G each thread will can use memory 3gb to 3.5g
#define THREAD_COUNT 2

typedef struct {
    char *chunk_start;
    char *chunk_end;
    int chunk_id;
    int number_of_lines;
    int line_number_start;
    int line_number_end;
} chunk_thread_data;

typedef struct 
{
    char in_file1[256];
    char in_file2[256];
    char out_file[256]; 
} merge_sort_data;
/*
    // Starting time for the clock
    auto start = high_resolution_clock::now();

    // Ending time for the clock
    auto stop = high_resolution_clock::now();

    auto duration = duration_cast<microseconds>(stop - start);
*/
static void* merge_sort_worker_thread(void *pthread_data) 
{
    char word1[256] = "";
    int count1 = 0;
    int ret1 = 1;
    char word2[256] = "";
    int count2 = 0;
    int ret2 = 1;
    int ret = 0;
    merge_sort_data* thread_data = (merge_sort_data*)pthread_data;
    FILE* fp1 = fopen(thread_data->in_file1, "r+");
    if (fp1 == NULL)
        return NULL;
    FILE* fp2 = fopen(thread_data->in_file2, "r+");
    if (fp2 == NULL)
        return NULL;
    FILE* fp = fopen(thread_data->out_file, "w+");
    if (fp == NULL)
        return NULL;
    int move1 = 1;
    int move2 = 1;
    while(ret1 != EOF && ret2 != EOF)
    {
        if (move1 == 1) {
            ret1 = fscanf(fp1, "%s %d\n", word1, &count1);
            if (ret1 == EOF) {
                break;
            }
            move1 = 0;
        }
        if (move2 == 1) {
            ret2 = fscanf(fp2, "%s %d\n", word2, &count2);
            if (ret2 == EOF) {
                break;
            }
            move2 = 0;
        }
        ret = strcmp(word1, word2);
        if (ret == 0) {
            move1 = 1;
            move2 = 1;
            fprintf(fp, "%s %d\n", word1, count1 + count2); 
        } else if (ret < 0) {
            move1 = 1;
            fprintf(fp, "%s %d\n", word1, count1); 
        } else {
            move2 = 1;
            fprintf(fp, "%s %d\n", word2, count2); 
        }
    }
    while(ret1 != EOF) {
        ret1 = fscanf(fp1, "%s %d\n", word1, &count1);
        if (ret1 != EOF) {
            fprintf(fp, "%s %d\n", word1, count1); 
        }
    }
    while(ret2 != EOF) {
        ret2 = fscanf(fp2, "%s %d\n", word2, &count2);
        if (ret2 != EOF) {
            fprintf(fp, "%s %d\n", word2, count2); 
        }
    }
    fclose(fp1);
    fclose(fp2);
    fclose(fp);
    return NULL;
}

static void* chunk_worker_thread(void *thread_data) 
{
    int word_count = 0;
    char file_1_path[256] = "";
    char file_2_path[256] = "";
    char word[256] = "";
    chunk_thread_data *cdata = (chunk_thread_data *)thread_data;
    sprintf(file_1_path, "%s_%d.txt", FILE_1_NAME, cdata->chunk_id);
    sprintf(file_2_path, "%s_%d.txt", FILE_2_NAME, cdata->chunk_id);
    //good to have default size preallocated : it is a bottleneck
    //this map is sorted by default by key operation < 
    //insertion in this map is order of log(n) where is n is current map size
    std::map<std::string, int> mymap; 
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
    for (int i = 0; i < THREAD_COUNT; i=+2) {
        merge_sort_data thread_mergedata;
        char path[256] = "";
        sprintf(path, "%s_%d.txt", FILE_2_NAME, i); 
        strcpy(thread_mergedata.in_file1, path);
        i++;
        sprintf(path, "%s_%d.txt", FILE_2_NAME, i); 
        strcpy(thread_mergedata.in_file2, path);
        sprintf(path, "merged_file_%d.txt", i); 
        strcpy(thread_mergedata.out_file, path);
        pthread_t thread;
        std::cout << " Calling merge thread" << std::endl;
        pthread_create(&thread, NULL, merge_sort_worker_thread, &thread_mergedata);
        //if (i == THREAD_COUNT - 1)
        pthread_join(thread, NULL);
        break;
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
