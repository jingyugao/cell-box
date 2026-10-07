#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

static const size_t memory_size = 32U * 1024U * 1024U;
static const char shared_value[] = "shared-memory-checkpoint";
static const char private_value[] = "private-memory-checkpoint";
static const char deleted_value[] = "deleted-open-checkpoint";

static void fail(const char *message) {
  perror(message);
  exit(1);
}

static void touch_file(const char *path) {
  int fd = open(path, O_CREAT | O_WRONLY | O_TRUNC, 0666);
  if (fd < 0) fail(path);
  if (write(fd, "x", 1) != 1) fail("write marker");
  if (close(fd) != 0) fail("close marker");
}

static void wait_for_file(const char *path) {
  for (;;) {
    if (access(path, F_OK) == 0) return;
    if (errno != ENOENT) fail("access control marker");
    usleep(50000);
  }
}

static unsigned long long memory_checksum(const unsigned char *memory) {
  unsigned long long checksum = 0;
  for (size_t i = 0; i < memory_size; i += 4096) checksum += memory[i];
  return checksum;
}

static long long resident_kib(void) {
  FILE *status = fopen("/proc/self/status", "r");
  if (!status) fail("open proc status");
  char *line = NULL;
  size_t capacity = 0;
  long long value = -1;
  while (getline(&line, &capacity, status) >= 0) {
    if (sscanf(line, "VmRSS: %lld kB", &value) == 1) break;
  }
  free(line);
  fclose(status);
  return value;
}

static void write_result(const char *path, const char *stage, unsigned char *memory) {
  FILE *out = fopen(path, "w");
  if (!out) fail(path);
  fprintf(out, "stage=%s\npid=%ld\nrss_kib=%lld\nchecksum=%llu\n",
          stage, (long)getpid(), resident_kib(), memory_checksum(memory));
  if (fclose(out) != 0) fail("close result");
}

static void make_payload(void) {
  const char *path = "/home/agent/payload-2g.bin";
  int fd = open(path, O_CREAT | O_WRONLY | O_TRUNC, 0666);
  if (fd < 0) fail(path);
  unsigned char *buffer = malloc(1024U * 1024U);
  if (!buffer) fail("malloc payload buffer");
  for (size_t i = 0; i < 1024U * 1024U; i++) {
    buffer[i] = (unsigned char)((i * 131U + 17U) & 0xffU);
  }
  for (int block = 0; block < 2048; block++) {
    size_t written = 0;
    while (written < 1024U * 1024U) {
      ssize_t n = write(fd, buffer + written, 1024U * 1024U - written);
      if (n < 0 && errno == EINTR) continue;
      if (n <= 0) fail("write payload");
      written += (size_t)n;
    }
  }
  if (fsync(fd) != 0) fail("fsync payload");
  if (close(fd) != 0) fail("close payload");
  free(buffer);
}

static int write_all(int fd, const char *value, size_t length) {
  size_t written = 0;
  while (written < length) {
    ssize_t n = write(fd, value + written, length - written);
    if (n < 0 && errno == EINTR) continue;
    if (n <= 0) return -1;
    written += (size_t)n;
  }
  return 0;
}

int main(int argc, char **argv) {
  if (argc != 3) {
    fprintf(stderr, "usage: persistent-home baseline|workload /control\n");
    return 2;
  }
  char ready_path[512], continue_path[512], pre_path[512], post_path[512];
  snprintf(ready_path, sizeof(ready_path), "%s/ready", argv[2]);
  snprintf(continue_path, sizeof(continue_path), "%s/continue", argv[2]);
  snprintf(pre_path, sizeof(pre_path), "%s/pre.txt", argv[2]);
  snprintf(post_path, sizeof(post_path), "%s/post.txt", argv[2]);

  unsigned char *memory = malloc(memory_size);
  if (!memory) fail("malloc resident memory");
  for (size_t i = 0; i < memory_size; i += 4096) memory[i] = (unsigned char)(i / 4096U % 251U);
  if (memory_checksum(memory) == 0) {
    fprintf(stderr, "resident-memory marker unexpectedly empty\n");
    return 3;
  }
  char root_marker[256];
  snprintf(root_marker, sizeof(root_marker), "/probe/root-overlay-%s.marker", argv[1]);
  touch_file(root_marker);

  if (strcmp(argv[1], "baseline") == 0) {
    write_result(pre_path, "baseline", memory);
    touch_file(ready_path);
    wait_for_file(continue_path);
    if (access(root_marker, F_OK) != 0) {
      fprintf(stderr, "guest root overlay marker was not preserved: %s\n", root_marker);
      return 20;
    }
    write_result(post_path, "baseline-restored", memory);
    return 0;
  }
  if (strcmp(argv[1], "workload") != 0) {
    fprintf(stderr, "unknown mode: %s\n", argv[1]);
    return 2;
  }

  make_payload();

  int offset_fd = open("/home/agent/offset.dat", O_CREAT | O_RDWR | O_TRUNC, 0666);
  if (offset_fd < 0) fail("open offset.dat");
  if (write_all(offset_fd, "0123456789abcdef", 16) != 0) fail("write offset.dat");
  if (lseek(offset_fd, 4, SEEK_SET) != 4) fail("seek offset.dat");

  int deleted_fd = open("/home/agent/deleted.dat", O_CREAT | O_RDWR | O_TRUNC, 0666);
  if (deleted_fd < 0) fail("open deleted.dat");
  if (write_all(deleted_fd, deleted_value, sizeof(deleted_value)) != 0) fail("write deleted.dat");
  if (unlink("/home/agent/deleted.dat") != 0) fail("unlink deleted.dat");

  int shared_fd = open("/home/agent/shared.dat", O_CREAT | O_RDWR | O_TRUNC, 0666);
  int private_fd = open("/home/agent/private.dat", O_CREAT | O_RDWR | O_TRUNC, 0666);
  if (shared_fd < 0 || private_fd < 0) fail("open mapping files");
  if (ftruncate(shared_fd, 4096) != 0 || ftruncate(private_fd, 4096) != 0) fail("size mapping files");
  char *shared_map = mmap(NULL, 4096, PROT_READ | PROT_WRITE, MAP_SHARED, shared_fd, 0);
  char *private_map = mmap(NULL, 4096, PROT_READ | PROT_WRITE, MAP_PRIVATE, private_fd, 0);
  if (shared_map == MAP_FAILED || private_map == MAP_FAILED) fail("mmap");
  memcpy(shared_map, shared_value, sizeof(shared_value));
  memcpy(private_map, private_value, sizeof(private_value));
  if (msync(shared_map, 4096, MS_SYNC) != 0) fail("msync shared mapping");

  write_result(pre_path, "ready", memory);
  touch_file(ready_path);
  wait_for_file(continue_path);

  char offset_read[6] = {0};
  if (read(offset_fd, offset_read, 5) != 5 || memcmp(offset_read, "45678", 5) != 0) {
    fprintf(stderr, "open file offset was not preserved: %.5s\n", offset_read);
    return 10;
  }
  char deleted_read[sizeof(deleted_value)] = {0};
  if (pread(deleted_fd, deleted_read, sizeof(deleted_value), 0) != sizeof(deleted_value) ||
      memcmp(deleted_read, deleted_value, sizeof(deleted_value)) != 0) {
    fprintf(stderr, "deleted-open file contents were not preserved\n");
    return 11;
  }
  if (memcmp(shared_map, shared_value, sizeof(shared_value)) != 0) {
    fprintf(stderr, "MAP_SHARED bytes were not preserved\n");
    return 12;
  }
  if (memcmp(private_map, private_value, sizeof(private_value)) != 0) {
    fprintf(stderr, "MAP_PRIVATE bytes were not preserved\n");
    return 13;
  }
  if (access(root_marker, F_OK) != 0) {
    fprintf(stderr, "guest root overlay marker was not preserved: %s\n", root_marker);
    return 14;
  }
  write_result(post_path, "restored", memory);
  return 0;
}
