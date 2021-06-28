(ns app.macros)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defmacro ->hash [& vars]
  (list `zipmap
    (mapv keyword vars)
    (vec vars)))

(defmacro cond-let
  "An alternative to `clojure.core/cond` where instead of a test/expression pair, it is possible
  to have a :let/binding vector pair."
  [& clauses]
  (cond (empty? clauses)
        nil

        (not (even? (count clauses)))
        (throw (ex-info (str `cond-let " requires an even number of forms")
                        {:form &form
                         :meta (meta &form)}))

        :else
        (let [[test expr-or-binding-form & more-clauses] clauses]
          (if (= :let test)
            `(let ~expr-or-binding-form (cond-let ~@more-clauses))
            ;; Standard case
            `(if ~test
               ~expr-or-binding-form
               (cond-let ~@more-clauses))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def field-names
  ["underlying_symbol"
   "quote_datetime"
   "root"
   "expiration"
   "strike"
   "option_type"
   "open"
   "high"
   "low"
   "close"
   "trade_volume"
   "bid_size"
   "bid"
   "ask_size"
   "ask"
   "underlying_bid"
   "underlying_ask"
   "implied_underlying_price"
   "active_underlying_price"
   "implied_volatility"
   "delta"
   "gamma"
   "theta"
   "vega"
   "rho"
   "open_interest"])
(def field-name-indexes (into {} (map-indexed (fn [i x] [x i]) field-names)))

(defmacro field [arr k]
  (let [i (get field-name-indexes k)
        transformer (case k
                      "delta" `(Double/parseDouble)
                      `(identity))]
    `(-> (aget ~arr ~i)
         ~@transformer)))
