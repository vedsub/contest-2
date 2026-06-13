import type { QueuedStep } from "../types/workflow"
import { redis } from "./redis-client"

const STEP_QUEUE_KEY = "workflow:step-queue"

/**
 * TODO: Implement this class.
 *
 * A FIFO queue backed by Redis.
 * Use LPUSH to enqueue and RPOP to dequeue.
 * Items are serialized as JSON strings in Redis.
 *
 * enqueue(step)  → redis.lpush(STEP_QUEUE_KEY, JSON.stringify(step))
 * dequeue()      → redis.rpop(STEP_QUEUE_KEY), parse and return or null if empty
 * peek()         → redis.lrange(STEP_QUEUE_KEY, -1, -1) without removing
 * size()         → redis.llen(STEP_QUEUE_KEY)
 */
export class StepQueue {
  async enqueue(step: QueuedStep): Promise<void> {
    throw new Error("TODO: implement enqueue")
  }

  async dequeue(): Promise<QueuedStep | null> {
    throw new Error("TODO: implement dequeue")
  }

  async peek(): Promise<QueuedStep | null> {
    throw new Error("TODO: implement peek")
  }

  async size(): Promise<number> {
    throw new Error("TODO: implement size")
  }
}

export const stepQueue = new StepQueue()
