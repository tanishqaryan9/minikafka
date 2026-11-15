package minikafka.web;

import minikafka.cluster.NotLeaderException;
import minikafka.cluster.ReplicationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.io.IOException;

//One place that turns broker exceptions into HTTP answers
@RestControllerAdvice
public class ApiExceptionHandler {

    //Bad input: invalid topic/group name, partition or offset out of range, ...
    @ExceptionHandler({IllegalArgumentException.class, ArrayIndexOutOfBoundsException.class})
    ProblemDetail badRequest(RuntimeException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, e.getMessage());
    }

    //Wrong state: consumer is not a member of the group (never joined / session expired), partition not owned, ...
    @ExceptionHandler(IllegalStateException.class)
    ResponseEntity<ProblemDetail> conflict(IllegalStateException e) {
        return ResponseEntity.status(HttpStatus.CONFLICT)
                .body(ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT, e.getMessage()));
    }

    //This node is a follower: tell the client where the leader is
    @ExceptionHandler(NotLeaderException.class)
    ResponseEntity<ProblemDetail> notLeader(NotLeaderException e) {
        ResponseEntity.BodyBuilder b = ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE);
        if (e.leaderAddress() != null) b.header("X-Leader-Address", e.leaderAddress());
        return b.body(ProblemDetail.forStatusAndDetail(HttpStatus.SERVICE_UNAVAILABLE, e.getMessage()));
    }

    //The record is stored on the leader but a majority of nodes did not confirm it
    @ExceptionHandler(ReplicationException.class)
    ResponseEntity<ProblemDetail> notReplicated(ReplicationException e) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(ProblemDetail.forStatusAndDetail(HttpStatus.SERVICE_UNAVAILABLE, e.getMessage()));
    }

    @ExceptionHandler(IOException.class)
    ResponseEntity<ProblemDetail> io(IOException e) {
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(ProblemDetail.forStatusAndDetail(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage()));
    }
}
