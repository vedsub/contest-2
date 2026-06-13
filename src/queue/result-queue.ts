import type { StepResult } from "../types/workflow"
import { redis } from "./redis-client"

const RESULT_QUEUE_KEY = "workflow:result-queue"

/**
 * TODO: Implement this class.
 *
 * A queue for results coming back from the pod manager.
 * Use LPUSH to push and BRPOP to consume (blocking pop).
 *
 * push(result)              → redis.lpush(RESULT_QUEUE_KEY, JSON.stringify(result))
 * consume(handler)          → loop forever using subscriber.brpop(RESULT_QUEUE_KEY, 0),
 *                             parse each result, call handler(result)
 *
 * The consume loop must run continuously in the background.
 * Use a dedicated ioredis connection for BRPOP via redis.duplicate() —
 * blocking calls must not share a connection with non-blocking operations.
 */
export class ResultQueue {
  async push(result: StepResult): Promise<void> {
    throw new Error("TODO: implement push")
  }

  async consume(handler: (result: StepResult) => Promise<void>): Promise<void> {
    throw new Error("TODO: implement consume")
  }
}

export const resultQueue = new ResultQueue()
