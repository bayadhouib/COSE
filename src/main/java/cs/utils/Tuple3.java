package cs.utils;

import java.util.Objects;

public class Tuple3<X, Y, Z> {
    public X _1;
    public Y _2;
    public Z _3;

    // Default no-argument constructor (required by Kryo)
    public Tuple3() {
    }

    // Constructor
    public Tuple3(X _1, Y _2, Z _3) {
        this._1 = _1;
        this._2 = _2;
        this._3 = _3;
    }

    // Getters
    public X _1() {
        return _1;
    }

    public Y _2() {
        return _2;
    }

    public Z _3() {
        return _3;
    }

    // toString method
    @Override
    public String toString() {
        return "Tuple3{" +
                "_1=" + _1 +
                ", _2=" + _2 +
                ", _3=" + _3 +
                '}';
    }

    // hashCode
    @Override
    public int hashCode() {
        return Objects.hash(_1, _2, _3);
    }

    // equals method
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Tuple3<?, ?, ?> tuple3 = (Tuple3<?, ?, ?>) o;

        return Objects.equals(_1, tuple3._1) &&
                Objects.equals(_2, tuple3._2) &&
                Objects.equals(_3, tuple3._3);
    }
}
