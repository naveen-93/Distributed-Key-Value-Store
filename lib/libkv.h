#ifndef LIBKV_H
#define LIBKV_H

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Initialize the key-value store client with a list of servers
 * @param server_list Null-terminated array of "host:port" strings
 * @return 0 on success, -1 on failure
 */
int kv_init(char **server_list);

/**
 * Shutdown the client and free resources
 * @return 0 on success, -1 on failure
 */
int kv_shutdown(void);

/**
 * Get value for a key
 * @param key Key to lookup (max 128 bytes)
 * @param value Buffer to store value (must be at least 2049 bytes)
 * @return 0 if key exists, 1 if key doesn't exist, -1 on failure
 */
int kv_get(char *key, char *value);

/**
 * Put value for a key
 * @param key Key to store (max 128 bytes)
 * @param value Value to store (max 2048 bytes)
 * @param old_value Buffer to store previous value if any (must be at least 2049 bytes)
 * @return 0 if key existed, 1 if key was new, -1 on failure
 */
int kv_put(char *key, char *value, char *old_value);

#ifdef __cplusplus
}
#endif

#endif /* LIBKV_H */ 