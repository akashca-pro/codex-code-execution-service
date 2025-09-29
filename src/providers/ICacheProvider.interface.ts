/**
 * Interface of the provider responsible for caching data.
 * 
 * @interface
 */
export interface ICacheProvider {

    /**
     * Retrieve the cached data based on provided key.
     * 
     * @param {string} key - The key of the cached data.
     * @returns {Promise<T | null>} - Returns the data based on key.
     */
    get<T>(key : string) : Promise<T | null> 

    /**
     * Cache the data based on a key.
     * 
     * @param {string} key - The key of the caching data.
     * @param value - The value to be cached.
     * @param ttl - The expiry time for the cache.
     * @returns {Promise<void>}
     */
    set<T>(key : string, value : T, ttl : number) : Promise<void>

    /**
     * Deleted the cached data.
     * 
     * @param {string} key - The key of the cached data.
     * @returns {Promise<void>}
     */
    del(key : string) : Promise<void>
}