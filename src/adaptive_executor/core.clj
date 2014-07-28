(ns adaptive-executor.core
  "An ExecutorService which uses an adaptively sized threadpool to optimize a
  particular observable.

  The goal here is to automatically determine the appropriate degree of
  parallelism for a threadpool executor under continuously changing conditions,
  via hillclimbing. We:

  0. Assume a fitness model: a function which maps the observable state of the
     executor to a scalar.

  1. Periodically measure the state of the executor, and maintain a window of
     the last N seconds of observations.

  2. Periodically make one of two adjustments:

     a. If the system is stable, make a small perturbation so we have more
        data about the local state space.

     b. If we have clean enough data about the local gradient, make a shift
        which attempts to improve the fitness function.

  Note that this problem is *asymmetric*. Keeping around extra threads is not
  the end of the world, but running *out* of threads unexpectedly is a serious
  problem. At the same time, we *don't* want to spawn unbounded new threads all
  the time; the fitness function may actually *decrease* with unbounded
  parallelism. Queuing may be optimal. We need *some* freedom to move
  around in order to understand the local gradient, which implies we should
  bias the system towards slightly *overprovisioned* regions of phase space,
  rather than risk severe nonlinearities when underprovisioned.

  Note that short periods of abysmal behavior may be tolerable (and expected).
  For instance, queuing 1e7 tasks behind a single thread might be catastrophic
  if allowed to last for a full second--but might be fine if the pool expands
  to 100 threads within, say, 50ms. Of course, if the downstream service has
  crashed, no amount of new threads will solve the problem, and we should only
  continue expanding if the gradient is positive.

  On the other hand, slow convergence is important. If there are multiple
  threadpools interacting in the system, we want to guarantee that they
  *collectively* find an optimal balance. That suggests that the timescale of
  changes in dynamics must be significantly longer than the period of the
  control loop.

  Note also that the load on the system may be rapidly time-varying. Queue
  depth in a *healthy* system might be expected to fluctuate wildly between 0
  and 1e4 elements. To compensate for time-dependent dynamics, we need to
  sample each point in phase space at *multiple times*. Because the fitness
  model may (and probably should) be nonlinear, it's important to apply the
  fitness function to the observation set directly, instead of taking the mean
  of observations at each point in phase space.

  Because time-dependent dynamics may be periodic, the sampling intervals for
  any given point in the phase space should be randomized. If the samples and
  dynamics are uncorrelated, we can extract a linear regression in *time* and
  use it to estimate new costs.

  Queue size and queue latency are both important bounds. Little's Law allows
  us to project mean latency from queue depth and present throughput. What of
  the execution latency profile? Mean latency is usually much less important
  than 99.9th--but where queue effects dominate, latency profiles are
  compressed towards the mean, so for our purposes mean latency (or
  equivalently, time-to-invocation as opposed to time-to-completion) may be
  fine."
  (:require [clojure.stacktrace :as trace])
  (:import (java.lang Runnable)
           (java.util.concurrent Callable
                                 ExecutorService
                                 Executor
                                 Future
                                 ThreadPoolExecutor
                                 TimeUnit
                                 LinkedBlockingQueue)))

(defprotocol IAdaptiveExecutor
  (^ThreadPoolExecutor executor [this])
  (controller [this])
  (fitness-fn [this])
  (interval [this])
  (measurements [this]))

(def decay-rate
  "How quickly observations fall off in strength, in weight/nanosecond."
  1e-10)

(def min-weight "The smallest weight still allowed for an observation." 0.1)

(defn weight
  "Determines the weight of an observation, given the current time in nanos."
  [now observation]
  (- 1 (* decay-rate (- now (:time observation)))))

(defn compute-weights
  "Returns a map of the given observations with their current weights attached."
  [observations]
  (let [now (System/nanoTime)]
    (mapv #(assoc % :weight (weight now %)) observations)))

(defn prune-observations
  "Removes observations with too low a weight."
  [observations]
  (filter #(<= min-weight (:weight %)) observations))

(defn weighted-average
  "Given a function to find the weight, a function to find the value, and a
  sequence of observations, returns a weighted average of values from those
  observations."
  [weight value observations]
  (/ (reduce + (map * (map weight observations) (map value observations)))
     (reduce + (map weight observations))))

(defn aggregate-fitnesses
  "Given a sequence of observations, returns a map of xs to

    {:y   fitness
     :dy  rate-of-fitness-change-per-nanosecond)"
  [observations]
  (->> observations
       (group-by :threads)
       (map (fn [[k vs]]
              (let [mean (weighted-average :weight :fitness vs)]
                [k {:fitness mean
                    :projected-fitness mean}])))
       (into (sorted-map))))

(defn analyze
  "Computes an analysis of an executor based on its observations."
  [executor]
  (->> executor
       measurements
       deref
       compute-weights
       prune-observations
       aggregate-fitnesses))

(defn measure
  "Takes a reading of an executor's state, returning a map with keys like :time, :queue-size, :threads-active, and so on."
  [ae]
  (let [tpe (executor ae)
        m   {:time           (System/nanoTime)
             :queue-size     (.. tpe getQueue size)
             :threads-active (.. tpe getActiveCount)
             :threads        (.. tpe getPoolSize)}
        fitness ((fitness-fn ae) m)]
    m (assoc m :fitness fitness)))

(defn go []
  (analyze [{:threads 0 :fitness 2 :time (- (System/nanoTime) 0e8)}
            {:threads 0 :fitness 1 :time (- (System/nanoTime) 5e8)}
            {:threads 0 :fitness 0 :time (- (System/nanoTime) 0)}]))

(defn set-pool-size!
  "Changes the adaptiveexecutor pool size. Blocks until threads started."
  [ae size]
  (let [ex (executor ae)]
    ; Ensure bounds don't cross
    (if (< size (.getMaximumPoolSize ex))
      (doto ex
        (.setCorePoolSize size)
        (.setMaximumPoolSize size))
      (doto ex
        (.setMaximumPoolSize size)
        (.setCorePoolSize size)))
    (.prestartAllCoreThreads ex)))

(def explore-shrink-factor
  "By what fraction should we shrink when randomly exploring?"
  0.95)

(def explore-grow-factor
  "By what fraction should we expand when randomly exploring?"
  1.05)

(defn explore-coordinate
  "Given an analysis map, returns a new random coordinate to explore."
  [analysis]
  (prn "exploring")
  (let [min-coord (key (first analysis))
        max-coord (key (last analysis))
        c1        (min (dec min-coord)
                       (int (* explore-shrink-factor min-coord)))
        c2        (max (inc max-coord)
                       (int (* explore-grow-factor max-coord)))
        d         (inc (- c2 c1))]
    (+ c1 (rand-int d))))

(defn optimal-coordinate
  "Given an analysis map, returns an estimated optimal coordinate."
  [analysis]
  (prn "optimizing")
  (->> analysis
       reverse ; Traverse from fewest threads to highest.
       (apply max-key (comp :projected-fitness val))
       key))

(def noise-ratio
  "If a coordinate arrives at value 1, noise-ratio 0.1 allows it to vary from
  0.95 to 1.05."
  0.05)

(defn noise
  "Given an optimal coordinate, picks a random one near it."
  [coordinate]
  (condp < (rand)
    0.9  (long (* 1.1 coordinate))
    0.66 (inc coordinate)
    0.33 coordinate
    0.1  (long (* 0.9 coordinate))
         (dec coordinate)))

(def min-threads 1)
(def max-threads 10000)

(defn bound-coordinate
  "Pins a coordinate to a valid range."
  [coordinate]
  (min max-threads (max min-threads coordinate)))

(def minimum-distinct-coordinates
  "How many different coordinates in the state space should we try to keep
  track of?"
  10)

(defn move!
  "Adjust the number of threads running."
  [ae]
  (let [analysis      (analyze ae)
        new-pool-size (bound-coordinate
                        (noise
                          (optimal-coordinate analysis)))]
;    (prn "Measurements:")
;    (clojure.pprint/pprint @(measurements ae))
    (prn "Analysis:")
    (clojure.pprint/pprint analysis)
    (prn "Moving to" new-pool-size)
    (set-pool-size! ae new-pool-size)
    (prn "State")
    (clojure.pprint/pprint (measure ae))))

(defn start-control-thread
  "Starts a control thread for this executor, which periodically takes
  measurements and adjusts the executor concurrency."
  [executor]
  (future
    (loop []
      (when (deref (controller executor) (interval executor) true)
        (try
          ; Take measurements
          (let [m (measure executor)]
            (swap! (measurements executor) conj m))

          ; Take action
          (move! executor)

          (catch Throwable t
            (println "Adaptive executor control thread caught")
            (trace/print-cause-trace t)
            (println)))

        (recur)))))

(deftype AdaptiveExecutor [^ThreadPoolExecutor executor
                           controller
                           fitness-fn
                           ^int interval
                           measurements]
  IAdaptiveExecutor
  (executor [this] executor)
  (controller [this] controller)
  (fitness-fn  [this] fitness-fn)
  (interval [this] interval)
  (measurements [this] measurements)

  Executor
  (execute [this command] (.execute executor command))

  ExecutorService
  (awaitTermination [this timeout unit]
    (.awaitTermination executor timeout unit))

  (invokeAll [this tasks] (.invokeAll executor tasks))

  (invokeAll [this tasks timeout unit]
    (.invokeAll executor tasks timeout unit))

  (invokeAny [this tasks] (.invokeAny executor tasks))

  (invokeAny [this tasks timeout unit]
    (.invokeAny executor tasks timeout unit))

  (isShutdown [this] (.isShutDown executor))

  (isTerminated [this] (.isTerminated executor))

  (shutdown [this]
    (.shutdown executor)
    (deliver controller false))

  (shutdownNow [this]
    (.shutdownNow executor)
    (deliver false controller))

  (^Future submit [this ^Callable task] ^Future (.submit executor task))

  (^Future submit [this ^Runnable task] ^Future (.submit executor task))

  (submit [this task result] ^Future (.submit executor task result)))

(defn default-fitness
  [state]
  (/ (+ (* 0.9 (:queue-size state))
        (* 0.1 (:threads state)))))

(defn adaptive-executor
  [fitness-fn queue-size]
  (let [core-pool-size 1
        max-pool-size 1
        keep-alive-time 1
        keep-alive-unit TimeUnit/DAYS
        queue (LinkedBlockingQueue. ^int queue-size)
        tpe (ThreadPoolExecutor.
              core-pool-size
              max-pool-size
              keep-alive-time
              keep-alive-unit
              (LinkedBlockingQueue. ^int queue-size))
        x (AdaptiveExecutor.
            tpe
            (promise)
            fitness-fn
            100
            (atom []))]
    (start-control-thread x)
    x))

(defn load! [ex]
  (dotimes [i 100000000]
    (Thread/sleep (rand 100))
    (.submit ex #(Thread/sleep 1000))))
