package raft.stm;

public interface Transition {

    boolean doTransition();
}
