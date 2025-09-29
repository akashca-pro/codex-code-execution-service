import redis from "@/config/redis";
import { ICacheProvider } from "./ICacheProvider.interface";

/**
 * Redis Cache Provider.
 * 
 * @class
 */
export class RedisCacheProvider implements ICacheProvider {

    readonly #_redis = redis

    async get<T>(key: string): Promise<T | null> {
        const data = await this.#_redis.get(key);
        return data ? JSON.parse(data) as T : null;
    }

    async set<T>(key: string, value: T, ttl: number): Promise<void> {
        const randomTtl = Math.floor(ttl + (Math.random() * 60));
        await this.#_redis.set(key,JSON.stringify(value),"EX",randomTtl);
    }

    async del(key: string): Promise<void> {
        await this.#_redis.del(key);
    }

}