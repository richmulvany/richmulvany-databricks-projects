import { useState, useRef, useEffect } from "react";

export default function Chat() {

  // const [messages, setMessages] = useState([]);
  const [input, setInput] = useState("");
  const [isTyping, setIsTyping] = useState(false);
  const [reasoning, setReasoning] = useState("");

  const [queryLogEnabled, setQueryLogEnabled] = useState(true);
  const [queryLogCopied, setQueryLogCopied] = useState(false);
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const [boxFocussed, setBoxFocussed] = useState(false);
  const [boxInput, setBoxInput] = useState(false);
  const [boxInputDelay, setBoxInputDelay] = useState(false);

  const messagesEndRef = useRef(null);
  const eventSourceRef = useRef(null);
  const reasoningContainerRef = useRef(null);
  const inputRef = useRef(null);

  const [sessions, setSessions] = useState(() => {
    const saved = localStorage.getItem("chat_sessions");
    return saved ? JSON.parse(saved) : [];
  });

  const [currentSessionId, setCurrentSessionId] = useState(null);
  const [editingSessionId, setEditingSessionId] = useState(null);
  const [editingTitle, setEditingTitle] = useState("");
  const activeSessionRef = useRef(null);
  const currentSession = sessions.find(s => s.id === currentSessionId);
  const messages = currentSession?.messages || [];

  const hasStarted = messages.length > 0 || isTyping;
  const [started, setStarted] = useState(false);
  
  const otherSessions = sessions.filter(s => s.id !== currentSessionId);

  const [mounted, setMounted] = useState(false);

  useEffect(() => {
    setMounted(true);
  }, []);

  useEffect(() => {
    if (sessions.length === 0) {
      newChat();
    } else if (!currentSessionId) {
      // commented out to load on homescreen
      // setCurrentSessionId(sessions[0].id);
    }
  }, [sessions]);

  useEffect(() => {
    localStorage.setItem("chat_sessions", JSON.stringify(sessions));
  }, [sessions]);
  
  const deleteSession = (id) => {

    const updated = sessions.filter(s => s.id !== id);

    setSessions(updated);

    if (currentSessionId === id) {
      setCurrentSessionId(updated.length ? updated[0].id : null);
    }

  };

  const newChat = () => {

    if (eventSourceRef.current) {
      eventSourceRef.current.close();
      setIsTyping(false);
    }

    const newSession = {
      id: Date.now(),
      title: "New Chat",
      messages: []
    };

    setSessions(prev => [newSession, ...prev]);
    setCurrentSessionId(newSession.id);
    setReasoning("");

  };

  useEffect(() => {
    document.body.classList.add("overflow-hidden");
    return () => document.body.classList.remove("overflow-hidden");
  }, []);

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

  useEffect(() => {
    activeSessionRef.current = currentSessionId;
  }, [currentSessionId]);

  useEffect(() => {
    if (reasoningContainerRef.current) {
      reasoningContainerRef.current.scrollTop =
        reasoningContainerRef.current.scrollHeight;
    }
  }, [reasoning]);

async function generateTitle(message) {

  try {

    const res = await fetch("http://localhost:8000/chat_title", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ message })
    });

    const title = await res.text();

    return title.slice(0,40);

  } catch {
    return message.slice(0,40);
  }

}

function highlightSQL(line) {

  let formatted = line;

  // Tables after FROM or JOIN
  formatted = formatted.replace(
    /\b(FROM|JOIN)\s+([a-zA-Z0-9_.]+)/gi,
    (match, keyword, table) =>
      `${keyword} <span style="color:var(--color-sql-table)">${table}</span>`
  );
  // Numbers
  formatted = formatted.replace(
    /\b\d+(\.\d+)?\b/g,
    `<span style="color:var(--color-sql-number)">$&</span>`
  );

  // Keywords
  formatted = formatted.replace(
    /\b([A-Z]+)\b/g,
    (match) =>
      `<span class="text-fuchsia-800 font-semibold">${match}</span>`
  );

  return formatted;
}

  const handleInput = (e) => {
    setInput(e.target.value);

    // Trigger "started" as soon as user types
    if (!started && e.target.value.trim() !== "") {
      setStarted(true);
      setBoxInput(true); 
      setTimeout(() => setBoxInput(false), 1000); // Reset after 1 seconds
      setTimeout(() => setBoxInputDelay(true), 200); // Delay 200ms
    }

    const el = inputRef.current;
    el.style.height = "auto";
    el.style.height = el.scrollHeight + "px";
  };

  const sendMessage = (preset) => {

    
    const userMessage = preset || input;

    if (!userMessage.trim()) return;

    if (!currentSessionId) {
      newChat();
      return;
    }

    if (!started) setStarted(true);

    setSessions(prev =>
      prev.map(session => {
        if (session.id !== activeSessionRef.current) return session;

        const newMessages = [...session.messages, { type: "user", text: userMessage }];

        return {
          ...session,
          title: session.title,
          messages: newMessages
        };
      })
    );

    if (currentSession?.title === "New Chat") {

      generateTitle(userMessage).then(title => {
        const cleanTitle = title.replace(/["'`]/g, "");

        setSessions(prev =>
          prev.map(s =>
            s.id === activeSessionId
              ? { ...s, title: cleanTitle }
              : s
          )
        );

      });

    }

    setInput("");
    setReasoning("");
    setIsTyping(true);

    if (inputRef.current) inputRef.current.style.height = "auto";

    const url =
      `http://localhost:8000/ask_stream?question=${encodeURIComponent(userMessage)}`;
    
    const activeSessionId = currentSessionId;

    const source = new EventSource(url);
    eventSourceRef.current = source;

    source.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);

        if (data.trace) {
          setReasoning(prev => prev + data.trace + "\n");
        }

        if (data.answer) {
          const fullText = data.answer;
          let i = 0;

          const typeInterval = setInterval(() => {
            i++;
          
            setSessions(prev =>
              prev.map(session => {
                if (session.id !== activeSessionId) return session;

                let updated = [...session.messages];

                if (!updated.length || updated[updated.length - 1].type !== "bot") {
                  updated = [...updated, { type: "bot", text: fullText.slice(0, i) }];
                } else {
                  updated = updated.map((m, idx) =>
                    idx === updated.length - 1
                      ? { ...m, text: fullText.slice(0, i) }
                      : m
                  );
                }

                return { ...session, messages: updated };
              })
            );

            if (i >= fullText.length) clearInterval(typeInterval);

          }, 20);

          source.close();
          setIsTyping(false);
        }

      } catch (err) {
        console.error("Parse error:", err);
      }
    };

    source.onerror = (err) => {
      console.error("SSE error:", err);
      source.close();
      setIsTyping(false);
    };
  };

  const stopResponse = () => {
    if (eventSourceRef.current) {
      eventSourceRef.current.close();
      setIsTyping(false);
    }
  };

  const handleKeyDown = (e) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      sendMessage();
    }
  };

  return (

    <div className="flex justify-centre h-screen bg-gray-800 text-black p-6 gap-6">

      {/* Query Toggle */}
      <div className="fixed top-8 right-8 z-50 inline-flex items-center gap-2">

        <label htmlFor="query-toggle"
        className={`text-sm cursor-pointer ${
          queryLogEnabled
          ? "text-gray-800" 
          : "text-main"
        }`}
        >
          See Log
        </label>

        <div className="relative inline-block w-11 h-6">

          <input
            id="query-toggle"
            type="checkbox"
            className="peer appearance-none w-full h-full bg-second border border-gray-800 rounded-full cursor-pointer checked:bg-third transition-colors duration-300"
            checked={queryLogEnabled}
            onChange={() => setQueryLogEnabled(prev => !prev)}
          />

          <label
            htmlFor="query-toggle"
            className="absolute top-0.5 left-0.5 w-5 h-5 bg-main border border-back rounded-full shadow-sm cursor-pointer transition-transform duration-300 peer-checked:translate-x-5"
          />

        </div>

      </div>

      {/* Sidebar */}
      <div className="fixed px-4 py-1 rounded-xl font-bold text-3xl text-main">
          HR Assistant
      </div>
      <div
        className={`top-20 py-14 w-[19rem] flex flex-col flex-initial transition-all duration-700`}
      >
      
        <div className={`rounded-2xl p-3 flex flex-col gap-2 " ${
          sidebarOpen && hasStarted
            ? "bg-main translate-y-0"
            : sidebarOpen && !hasStarted
              ? "bg-main"
              : !sidebarOpen && !hasStarted
                ? "bg-none "
                : ""
        }`}
        >
          <div className="flex flex-row gap-3 mr-4 mt-1">
            <button
              onClick={() => setSidebarOpen(prev => !prev)}
              className={`flex ml-1 rounded-lg text-2xl ${
                sidebarOpen
                  ? !hasStarted
                    ? "hover:text-second"
                    : "hover:text-second"
                  : !hasStarted
                    ? "text-main hover:text-third"
                    : "text-main hover:text-third"
              }`}
            >
              ☰
            </button>
            <button x
            onClick={newChat}
            className={`border border-gray-800 rounded-lg p-2 text-black font-semibold w-full w-min-128 flex ${
              hasStarted
                ? sidebarOpen
                  ? "bg-second hover:bg-third"
                  : "bg-third hover:bg-second"
                : "opacity-0 pointer-events-none"
            }`}
            >
              + New Chat
            </button>
          </div>
          <div className={`flex flex-col gap-2 mt-2 transition-all duration-700 ${
            sidebarOpen
              ? "opacity-100"
              : "opacity-0 pointer-events-none"
          }`}
          >
            
            {sidebarOpen && (
              <div>
              {/* Current Chat */}
              {currentSession && (
                <div className="flex p-2 text-sm font-semibold border-b border-main">
                  {currentSession.title || "New Chat"}
                </div>
              )}

              {/* Other Sessions */}
              {otherSessions.map(session => (

                <div
                  key={session.id}
                  className="flex flex-row group rounded-lg hover:bg-third "
                >

                  {editingSessionId === session.id ? (

                    <input
                      autoFocus
                      value={editingTitle}
                      onChange={(e) => setEditingTitle(e.target.value)}
                      onBlur={() => {
                        setSessions(prev =>
                          prev.map(s =>
                            s.id === session.id ? { ...s, title: editingTitle } : s
                          )
                        );
                        setEditingSessionId(null);
                      }}
                      onKeyDown={(e) => {
                        if (e.key === "Enter") e.target.blur();
                      }}
                      className="text-sm p-2 flex-col flex-1 bg-transparent outline-none"
                    />

                  ) : (

                    <button
                      title={session.title}
                      onClick={() => {
                        setCurrentSessionId(session.id);
                        setReasoning("");
                      }}
                      onDoubleClick={() => {
                        setEditingSessionId(session.id);
                        setEditingTitle(session.title || "");
                      }}
                      className="text-left text-sm p-2 flex-1 truncate"
                    >
                      {session.title || "New Chat"}
                    </button>

                  )}

                  {/* delete button */}
                  <button
                    onClick={() => deleteSession(session.id)}
                    className="w-6 opacity-0 mr-1 group-hover:opacity-100 text-gray-800 font-bold text-sm"
                  >
                    ✕
                  </button>

                </div>

              ))}
            </div>)}
          </div>
        </div>

      </div>

      {/* Chat Container */}
      <div className={`flex flex-col flex-1 items-center max-w-5xl w-full transition-opacity duration-300 ${
        boxInput
          ? "opacity-0" : ""
        }`}>
        <div
          className={`w-16 h-64 bg-gray-800 flex flex-col transition-all duration-700 ${
            !started
              ? ""
              : "w-0 h-0"
          }`}
          >

        </div>
        <div
          className={`w-full flex flex-col transition-transform duration-700 ease-in-out will-change-transform
            ${started && mounted ? "max-w-3xl mt-auto" : "max-w-5xl"}
          `}
        >
          {/* Hero Section */}
          {!started && (
            <div className="text-center mb-4 select-none transition-opacity duration-700">
              <h1 className="text-5xl text-main mb-3">Ask About HR Analytics</h1>
              <div className="flex flex-wrap justify-center gap-3 mt-6">
                {[
                  "Which department has the highest attrition?",
                  "Show average age by department",
                  "Compare attrition between genders",
                  "Which job roles have the most attrition?"
                ].map((chip, i) => (
                  <button
                    key={i}
                    onClick={() => sendMessage(chip)}
                    className="px-4 py-2 bg-none border border-main rounded-full text-sm text-main hover:border-second transition"
                  >
                    {chip}
                  </button>
                ))}
              </div>
            </div>
          )}

          {/* Messages */}
          <div
            className="flex flex-col space-y-3 px-6 transition-all duration-700"
            style={{
              maxHeight: "calc(100vh - 150px)",
              overflowY: hasStarted ? "auto" : "visible",
            }}
          >
            {messages.map((msg, i) => (
              <div
                key={i}
                className={`p-3 rounded-xl max-w-md break-words transition-all duration-300 ${
                  msg.type === "user" ? "bg-second text-black ml-auto" : "bg-main text-black"
                }`}
              >
                {msg.text}
              </div>
            ))}

            {isTyping && (
              <div className="p-3 rounded-xl bg-main flex space-x-2 w-fit">
                <span className="w-3 h-3 bg-third border border-back rounded-full animate-bounce"></span>
                <span className="w-3 h-3 bg-third border border-back rounded-full animate-bounce delay-150"></span>
                <span className="w-3 h-3 bg-third border border-back rounded-full animate-bounce delay-300"></span>
              </div>
            )}

            <div ref={messagesEndRef} />
          </div>

          {/* Input */}
          <div className="p-4 flex w-full rounded-4xl bg-gray-800 transition-all duration-700">
            <textarea
              onFocus={() => {
              setBoxFocussed(true); 
              setTimeout(() => setBoxFocussed(false), 1000); // Reset after 1 seconds
              }}
              ref={inputRef}
              rows={1}
              className={`flex-1 p-3 rounded-xl text-black bg-main resize-none overflow-hidden max-h-32
                focus:outline focus:outline-2 focus:outline-offset-2 focus:outline-third placeholder-back
                transition-all duration-700
                ${input.length > 0 ? "mr-4" : ""}`}
              value={input}
              onChange={handleInput}
              onKeyDown={handleKeyDown}
              placeholder="Ask a question..."
            />

            {/* Send Button */}
            <button
              onClick={sendMessage}
              className={`flex items-center justify-center rounded-xl
                transition-all duration-700
                ${input.length > 0 ? "w-16 opacity-100" : "w-0 opacity-0 overflow-hidden"}`}
            >
              <div className="w-16 h-[46px] bg-second hover:bg-third rounded-xl flex items-center justify-center active:scale-90 outline-third">
                <svg
                  xmlns="http://www.w3.org/2000/svg"
                  viewBox="-1.95 -4.4 10 10"
                  fill="none"
                  strokeWidth={0.4}
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  className="stroke-main hover:stroke-second w-10 h-10"
                >
                  <path d="m0 0 0 2c0 1 1 1 1 1l4 0c1 0 1-1 1-1l0-4 1 1-1-1-1 1 1-1 0 0" />
                </svg>
              </div>
            </button>
          </div>
        </div>
      </div>

      {/* Query Log */}
      <div
        className={`group transition-all duration-700 w-80 flex flex-col flex-initial ${
          queryLogEnabled && (reasoning.length > 0)
            ? "opacity-100 translate-x-0"
            : hasStarted
              ? "opacity-0 translate-x-10 pointer-events-none"
              : "opacity-0 translate-x-0 pointer-events-none"
        }`}
      >
        <div
          ref={reasoningContainerRef}
          className="group bg-main rounded-2xl px-3 py-[0.4rem] overflow-y-auto min-h-16"
        >
          <h2 className="text-lg font-semibold mb-2 text-gray-900">
            Query Log
          </h2>

          <div className="text-xs text-gray-900 mb-[0.4rem]">

            {reasoning.split("\n").map((line, idx) => {

              if (line.startsWith("###### ")) {
                return (
                  <h5 key={idx} className="font-bold text-gray-600">
                    {line.replace("###### ", "")}
                  </h5>
                );
              }

              return (
                <div
                  key={idx}
                  dangerouslySetInnerHTML={{
                    __html: highlightSQL(line)
                  }}
                />
              );

            })}
          

          </div>

        </div>

        <div
        className="flex flex-row max-h-32 px-3 gap-2"
        >
          <button
            onClick={async () => {
              await navigator.clipboard.writeText(reasoning);
              setQueryLogCopied(true); 
              setTimeout(() => setQueryLogCopied(false), 3000); // Reset after 3 seconds
            }}
            className={`text-[1.7rem] font-bold text-main flex flex-row hover:text-third opacity-0 pointer-events-none group-hover:opacity-100 group-hover:pointer-events-auto transition-opacity duration-300 ${
              reasoning.length > 0
              ? ""
              : "opacity-0 pointer-events-none"
            }`}
          >
            ⎘
          </button>
          <div className={`text-[0.6rem] text-main opacity-0 pointer-events-none transition-opacity duration-300 flex flex-row mt-4 ${
            queryLogCopied 
            ? "opacity-100" : ""
            }`}
          >
            <span>log copied to clipboard</span>
            </div>

        </div>

        <div
        className="flex flex-row flex-1 w-8"
        >
        </div>

      </div>

    </div>
  );
}
