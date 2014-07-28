(ns adaptive-executor.lerning
  (require [clojure.core.matrix :as m]))

; We don't need a sophisticated model for hillclimbing; a quadratic polynomial
; should be more than sufficient, and the maxima have trivial closed-form
; expressions.

; We're looking for a parameter vector *beta* such that
;
; y = Xb + e
;
; where y is the observation vector, X is the augmented design matrix, b are
; coefficients for each column of X, and e is an error vector.

(defn poly-design-matrix
  "Given a polynomial degree (say 2 for a quadratic model), and a vector x of
  independent observations [x1 x2 x3 ...], generate a matrix like:

  [1 x1 x1^2]
  [1 x2 x2^2]
  [1 x3 x3^2]"
  [degree x]
  (->> (range (inc degree))
       (map (partial m/pow x))
       m/matrix
       m/transpose))

(defn solve
  "Given an observation vector y and a design matrix X, solves the
  least-squares linear regression problem, yielding a weight vector b such that
  y = Xb + e, and e is minimized."
  [y X]
  (let [X' (m/transpose X)]
    (m/mmul (m/inverse (m/mmul X' X)) X' y)))
