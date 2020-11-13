package raft.stm;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.HashMap;
import java.util.Map;

public class RaftStateMachine {

    private static final Log LOG = LogFactory.getLog(RaftStateMachine.class);
    private Map<RaftState, Map<RaftEvent, InternalTransition>> stateMachine = new HashMap<>();
    private volatile RaftState currentState;

    public RaftStateMachine(RaftState state) {
        this.currentState = state;
    }

    public RaftStateMachine addTransition(RaftState from, RaftEvent event, RaftState to, Transition hook) {
        Map<RaftEvent, InternalTransition> map = stateMachine.get(from);
        if (map == null) {
            map = new HashMap<>();
            map.put(event, new InternalTransition(to, hook));
            stateMachine.put(from, map);
        } else {
            map.put(event, new InternalTransition(to, hook));
        }
        return this;
    }

    public synchronized boolean onEvent(RaftEvent event) {
        InternalTransition transition = stateMachine.get(currentState).get(event);
        if (transition != null) {
            RaftState state = transition.doTransition();
            if (state != null) {
                currentState = state;
            }
            return true;
        } else {
            LOG.warn(currentState + " IGNORE EVENT " + event);
            return false;
        }
    }

    public RaftState getCurrentState() {
        return currentState;
    }

    public static class InternalTransition {
        private RaftState targetState;
        private Transition hook;

        public InternalTransition(RaftState state, Transition hook) {
            this.targetState = state;
            this.hook = hook;
        }

        public RaftState doTransition() {
            boolean success = false;
            if (hook != null) {
                success = hook.doTransition();
            }
            if (success) {
                return targetState;
            } else {
                return null;
            }
        }
    }

    public static void main(String[] args) {
    }
}
