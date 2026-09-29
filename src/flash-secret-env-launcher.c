#define _POSIX_C_SOURCE 200809L

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#ifndef FLASH_SECRET_DIR
#define FLASH_SECRET_DIR "/vault/secrets"
#endif

#define MAX_SECRET_VALUE_BYTES 16384
#define MAX_ENV_NAME_BYTES 253
#define MAX_SECRET_NAME_BYTES 63

static int valid_env_name(const char *name, size_t len) {
    if (len == 0 || len > MAX_ENV_NAME_BYTES ||
        !((name[0] >= 'A' && name[0] <= 'Z') ||
          (name[0] >= 'a' && name[0] <= 'z') || name[0] == '_')) {
        return 0;
    }
    for (size_t i = 1; i < len; i++) {
        char c = name[i];
        if (!((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
              (c >= '0' && c <= '9') || c == '_')) {
            return 0;
        }
    }
    return 1;
}

static int valid_secret_name(const char *name) {
    size_t len = strlen(name);
    if (len == 0 || len > MAX_SECRET_NAME_BYTES ||
        name[0] < 'a' || name[0] > 'z') {
        return 0;
    }
    for (size_t i = 1; i < len; i++) {
        char c = name[i];
        if (!((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-')) {
            return 0;
        }
    }
    return name[len - 1] != '-';
}

static int load_secret(const char *mapping) {
    const char *separator = strchr(mapping, '=');
    if (separator == NULL || !valid_env_name(mapping, (size_t)(separator - mapping)) ||
        !valid_secret_name(separator + 1)) {
        fputs("invalid secret environment mapping\n", stderr);
        return -1;
    }

    char env_name[MAX_ENV_NAME_BYTES + 1];
    size_t name_len = (size_t)(separator - mapping);
    memcpy(env_name, mapping, name_len);
    env_name[name_len] = '\0';

    char path[sizeof(FLASH_SECRET_DIR) + MAX_SECRET_NAME_BYTES + 2];
    if (snprintf(path, sizeof(path), "%s/%s", FLASH_SECRET_DIR, separator + 1) >=
        (int)sizeof(path)) {
        fputs("invalid secret path\n", stderr);
        return -1;
    }

    int fd = open(path, O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
    if (fd < 0) {
        fprintf(stderr, "secret for %s is unavailable\n", env_name);
        return -1;
    }
    char value[MAX_SECRET_VALUE_BYTES + 2];
    size_t length = 0;
    while (length < sizeof(value) - 1) {
        ssize_t read_bytes = read(fd, value + length, sizeof(value) - 1 - length);
        if (read_bytes < 0 && errno == EINTR) {
            continue;
        }
        if (read_bytes <= 0) {
            if (read_bytes < 0) {
                close(fd);
                fprintf(stderr, "secret for %s could not be read\n", env_name);
                return -1;
            }
            break;
        }
        length += (size_t)read_bytes;
    }
    close(fd);
    if (length == 0 || length > MAX_SECRET_VALUE_BYTES ||
        memchr(value, '\0', length) != NULL) {
        fprintf(stderr, "secret for %s has an invalid value\n", env_name);
        return -1;
    }
    value[length] = '\0';
    if (setenv(env_name, value, 1) != 0) {
        fprintf(stderr, "secret for %s could not be installed\n", env_name);
        return -1;
    }
    memset(value, 0, sizeof(value));
    return 0;
}

int main(int argc, char **argv) {
    int argument = 1;
    while (argument < argc && strcmp(argv[argument], "--secret-env") == 0) {
        argument++;
        if (argument >= argc || load_secret(argv[argument]) != 0) {
            return EXIT_FAILURE;
        }
        argument++;
    }
    if (argument >= argc || strcmp(argv[argument], "--") != 0 ||
        ++argument >= argc || argv[argument][0] == '\0') {
        fputs("missing workload command\n", stderr);
        return EXIT_FAILURE;
    }
    execvp(argv[argument], argv + argument);
    fprintf(stderr, "workload command could not start: %s\n", strerror(errno));
    return EXIT_FAILURE;
}
