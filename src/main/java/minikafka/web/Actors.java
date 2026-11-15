package minikafka.web;

import jakarta.servlet.http.HttpServletRequest;

//Builds the "by whom" string for the audit log of a REST call: rest:<client id>@<remote address>.
//Callers identify themselves with the X-Client-Id header; without it they show up as "anonymous".
final class Actors {
    static final String HEADER = "X-Client-Id";

    private Actors() {}

    static String rest(String clientId, HttpServletRequest request) {
        String id = clientId == null || clientId.isBlank() ? "anonymous" : clientId.trim();
        return "rest:" + id + "@" + request.getRemoteAddr();
    }
}
