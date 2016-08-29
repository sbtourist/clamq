(ns clamq.internal.macros)

(defmacro validate 
  "Validates value is not nil. If it is, throw IllegalArgumentException with failed-msg"
  [value failed-msg]
  `(when (nil? ~value)
     (throw (IllegalArgumentException. ~failed-msg))))
    
(defmacro non-blocking-listener [listener-class listener-method converter handler-fn failure-fn limit container]
  `(let [~'counter (atom 0)]
     (proxy [~listener-class] []
       (~listener-method [~'message]
         (try
           (let [~'converted (.fromMessage ~converter ~'message)]
             (swap! ~'counter inc)
             (try
               (~handler-fn ~'converted)
               (catch Exception ~'ex
                 (~failure-fn {:message ~'converted :exception ~'ex}))
               (finally
                 (if (= ~limit ~'@counter) (do (.stop ~container) (future (.shutdown ~container)))))))
           (catch Exception ~'ex
             (~failure-fn {:message ~'message :exception ~'ex})))))))

(defmacro blocking-listener [listener-class listener-method converter request-queue reply-queue container]
  `(proxy [~listener-class] []
     (~listener-method [~'message]
       (.put ~request-queue (.fromMessage ~converter ~'message))
       (loop []
         ; Is spinning really the better option?
         (let [~'m (.poll ~reply-queue 1000 java.util.concurrent.TimeUnit/MILLISECONDS)]
           (cond
             (and (nil? ~'m) (.isRunning ~container)) (recur)
             (and (nil? ~'m) (not (.isRunning ~container))) (throw (RuntimeException.))
             (= :rollback ~'m) (throw (RuntimeException.))
             :else nil))))))
