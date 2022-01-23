

/*
 * Copyright Ram Nindra (ramnindra@yahoo.com)
 */

#include <iostream>
#include <fcntl.h>
#include <pthread.h>
#include <sys/mman.h>
#include <sys/stat.h>

#define THREAD_COUNT 4

typedef struct {
    char *chunk_start;
    char *chunk_end;
    int chunk_id;
} chunk_thread_data;

static void* chunk_worker_thread(void *thread_data) 
{
    chunk_thread_data *cdata = (chunk_thread_data *)thread_data;
    int i = 0;
    while(cdata->chunk_start[i] != '\n') {
        cdata->chunk_start[i] = 'A'  + cdata->chunk_id;
        i++;
    }
    std::cout << "start = " << static_cast<void*>(cdata->chunk_start) << " ";
    std::cout << "end = " << static_cast<void*>(cdata->chunk_end) << std::endl;
    return NULL;
}

bool process_file(char* path)
{
    pthread_t threads[THREAD_COUNT];
    chunk_thread_data thread_data[THREAD_COUNT];

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

    std::cout << "total_bytes " << total_bytes << std::endl;

    char *chunks[THREAD_COUNT];
    for (int i = 0; i < THREAD_COUNT; i++) {
        chunks[i] = mmap_addr_start + (total_bytes / THREAD_COUNT) * i;
        if (i > 0) {
            while (*chunks[i] != '\n') {
                chunks[i]++;
            }
            chunks[i]++;
        }
        std::cout << "Addr Chunk Thread " << i << " : " << static_cast<void*>(chunks[i]) << std::endl;
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
    //Wait for worker threads to finish their job
    for (int i = 0; i < THREAD_COUNT; i++) {
        pthread_join(threads[i], NULL);
    }

    //munmap
    if (munmap(mmap_addr_start, total_bytes) == -1) {
        return false;
    }
    return true;
}

int main(int argc, char *argv[]) 
{
    if (argc != 2) 
    {
        std::cout << "Not correct number of argments passed\n";
        return -1;
    }
    process_file(argv[1]);
    return 0;
} 
