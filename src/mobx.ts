import { observable, runInAction } from "mobx";
import {
  createDistributedMap,
  type DistributedMap,
  type DistributedMapChange,
  type DistributedMapChangeListener,
  type DistributedMapKeyChangeListener,
  type DistributedMapOptions,
} from "./index.js";

/**
 * Creates a distributed map whose read methods participate in MobX tracking.
 *
 * Values are stored shallowly: replacing, adding, or deleting a map entry is
 * observable, while the value itself is not recursively converted by MobX.
 */
export async function createMobxDistributedMap<T>(
  name: string,
  options: DistributedMapOptions<T>,
): Promise<DistributedMap<T>> {
  const source = await createDistributedMap<T>(name, options);
  const values = observable.map<string, T>(new Map(source.entries()), {
    deep: false,
    name: `DistributedMap:${name}`,
  });
  let disposed = false;
  let remoteScheduled = false;
  const remoteChanges = new Map<string, DistributedMapChange<T>>();

  const applyChange = (change: DistributedMapChange<T>): void => {
    if (change.operation === "delete") {
      values.delete(change.key);
    } else {
      values.set(change.key, change.value as T);
    }
  };

  const applyRemoteChanges = (): void => {
    if (disposed || remoteChanges.size === 0) {
      remoteChanges.clear();
      remoteScheduled = false;
      return;
    }

    const changes = [...remoteChanges.values()];
    remoteChanges.clear();
    remoteScheduled = false;
    runInAction(() => {
      for (const change of changes) applyChange(change);
    });
  };

  const unsubscribe = source.onChange((change) => {
    if (change.source === "local") {
      runInAction(() => applyChange(change));
      return;
    }

    remoteChanges.set(change.key, change);
    if (!remoteScheduled) {
      remoteScheduled = true;
      queueMicrotask(applyRemoteChanges);
    }
  });

  const map: DistributedMap<T> = {
    name: source.name,
    get size() {
      return values.size;
    },
    get(key) {
      return values.get(key);
    },
    has(key) {
      return values.has(key);
    },
    entries() {
      return values.entries();
    },
    keys() {
      return values.keys();
    },
    values() {
      return values.values();
    },
    forEach(callback) {
      values.forEach((value, key) => callback(value, key));
    },
    [Symbol.iterator]() {
      return values.entries();
    },
    set(key, value) {
      source.set(key, value);
    },
    delete(key) {
      return source.delete(key);
    },
    clear() {
      source.clear();
    },
    onChange(
      keyOrListener: string | DistributedMapChangeListener<T>,
      keyListener?: DistributedMapKeyChangeListener<T>,
    ): () => void {
      if (typeof keyOrListener === "function") {
        return source.onChange(keyOrListener);
      }
      if (keyListener === undefined) {
        throw new TypeError("A key change listener is required");
      }
      return source.onChange(keyOrListener, keyListener);
    },
    flush() {
      return source.flush();
    },
    async synchronize() {
      await source.synchronize();
      applyRemoteChanges();
    },
    async destroy() {
      if (!disposed) {
        disposed = true;
        unsubscribe();
        remoteChanges.clear();
      }
      await source.destroy();
    },
  };

  return map;
}
