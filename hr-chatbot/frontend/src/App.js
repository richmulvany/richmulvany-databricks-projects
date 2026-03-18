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
  const [shrink, setShrink] = useState(false);

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
  const showHero = messages.length === 0 && !isTyping;

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

  const newChatButton = () => {
    if (sidebarOpen) {
      return "+ New Chat";}
    if (!sidebarOpen) {
      return "+";}

  }

  const newChat = () => {

    if (eventSourceRef.current) {
      eventSourceRef.current.close();
      setIsTyping(false);
    }

    setCurrentSessionId(null);
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

    const el = inputRef.current;
    el.style.height = "auto";
    el.style.height = el.scrollHeight + "px";
  };

  const sendMessage = (preset) => {

    
    const userMessage = preset || input;

    if (!userMessage.trim()) return;

  
    setBoxInput(true);
    setTimeout(() => setBoxInput(false), 500); 
    setTimeout(() => setBoxInputDelay(true), 0);
    

    let activeSessionId = currentSessionId;

    if (!activeSessionId) {

      const newSession = {
        id: Date.now(),
        title: "New Chat",
        messages: []
      };

      activeSessionId = newSession.id;

      setSessions(prev => [...prev, newSession]);
      setCurrentSessionId(activeSessionId);
    }

    setSessions(prev =>
      prev.map(session => {

        if (session.id !== activeSessionId) return session;

        const newMessages = [...session.messages, { type: "user", text: userMessage }];

        // trigger title generation only once
        if (session.title === "New Chat") {

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

        return {
          ...session,
          messages: newMessages
        };

      })
    );

    setInput("");
    setReasoning("");
    
    setIsTyping(true);

    if (inputRef.current) inputRef.current.style.height = "auto";

    const url =
      `http://localhost:8000/ask_stream?question=${encodeURIComponent(userMessage)}`;


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
          reasoning.length === 0 
          ? "text-main" 
          : !queryLogEnabled
            ? "text-main"
            : "text-gray-800"
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
      <div className="bg-none flex flex-col min-w-[19rem]">
        {/* Title */}
        <div className="fixed px-4 mt-0 rounded-xl font-bold text-3xl text-main">
          HR Assistant
        </div>

        {/* Sidebar */}
        <div
          className={`top-20 py-[4.25vh] flex flex-col flex-initial transition-all duration-500 truncate
          ${sidebarOpen ? "w-[19rem]" : "w-14"}
        `}
        >
          <div className="bg-none flex flex-col min-w-[19rem]">
            {/* Controls — always visible */}
            <div className="flex flex-row mt-5 gap-3 mr-10 max-h-12  px-3">
              <button
                onClick={() => {setSidebarOpen(prev => !prev); setShrink(true); setTimeout(() => setShrink(false), 700);}}
                className={`z-50 flex ml-1 rounded-lg text-2xl text-main transition-transform duration-300 ${
                  sidebarOpen ? "hover:text-second rotate-90" : "hover:text-third rotate-0"
                }`}
              >
                ☰
              </button>

              <button
                onClick={newChat}
                className={`transition-transform duration-700 ease-in-out
                ${
                  hasStarted
                    ? sidebarOpen
                      ? "rounded-xl p-2 text-black text-nowrap bg-third hover:bg-second font-semibold w-full flex translate-x-0 opacity-100 "
                      : shrink
                        ? "z-0 fixed px-10 py-[0.09vh] bg-opacity-0 hover:text-third text-main text-2xl"
                        : "z-0 fixed px-10 py-[0.09vh] bg-opacity-0 hover:text-third text-main text-2xl"
                    : "opacity-0 pointer-events-none"
                }`}
              >
                {newChatButton()}
              </button>
            </div>
          </div>
          {/* Animated panel */}
          <div
            className={`rounded-2xl p-3 flex flex-col gap-2 mt-4
            transition-all duration-300 ease-[cubic-bezier(.34,1.56,1.34,1)]
            origin-left overflow-y-auto scrollbar-side
            ${
              sidebarOpen
                ? "bg-main  opacity-100 "
                : "bg-main  -translate-y-10 -translate-x-10 opacity-0 pointer-events-none"
            }`}
          >

            {/* Chat Sessions */}
            {[...sessions].reverse().map((session, i) => {

              const isCurrent = session.id === currentSessionId;

              return (

                <div
                  key={session.id}
                  style={{ transitionDelay: `${i * 20}ms` }}
                  className={`flex flex-row group rounded-lg
                  transition-all duration-100
                  ${
                    sidebarOpen
                      ? "opacity-100 translate-x-0"
                      : "opacity-0 -translate-x-2"
                  }
                  ${!isCurrent ? "hover:bg-third" : "bg-second"}
                  `}
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
                      if (!isCurrent) {
                        setCurrentSessionId(session.id);
                        setReasoning("");
                      }
                    }}
                    onDoubleClick={() => {
                      setEditingSessionId(session.id);
                      setEditingTitle(session.title || "");
                    }}
                    className={`text-left text-sm p-2 flex-1 truncate
                      ${isCurrent ? "font-bold" : ""}
                    `}
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
              );
            })}
          </div>
        </div>
      </div>
      {/* Chat Container */}

      <div className={`flex flex-col flex-1 items-center max-w-5xl w-full transition-opacity duration-300 ${
              hasStarted && mounted ? "max-w-3xl mt-auto" : "max-w-5xl"}
            }`}
      >
        <div
          className={`w-full flex flex-col mt-auto transition-all duration-700 ease-in-out will-change-transform
            ${hasStarted && mounted ? "max-w-3xl mt-auto" : "max-w-5xl"} 
          `}
        >
          <div className={`absolute inset-0 z-50 bg-gray-800 transition-opacity duration-500 ${
            boxInput
              ? "opacity-40" : "opacity-0 pointer-events-none"
          }`}
          >
          </div>
          {/* Hero Section */}
          {showHero && (
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
            className="flex flex-col space-y-3 px-6 transition-all duration-700 scrollbar-chat"
            style={{
              maxHeight: "calc(100vh - 150px)",
              overflowY: hasStarted ? "scroll" : "visible",
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
                transition-all duration-700 ease-in-out
                  ${boxInputDelay
                    ? "mb-0" : "mb-[40vh]"
                  }`}
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
        className={`group w-80 flex flex-col flex-initial transition-all duration-300
          ease-[cubic-bezier(.34,1.56,1.34,1)] origin-top-right ${
          queryLogEnabled && (reasoning.length > 0)
            ? "opacity-100 translate-x-0"
            : hasStarted
              ? "opacity-0 translate-x-10 pointer-events-none"
              : "opacity-0 translate-x-0 pointer-events-none"
        }`}
      >
        <div
          ref={reasoningContainerRef}
          className="group bg-main rounded-2xl px-3 py-[0.75vh] overflow-y-auto scrollbar-query min-h-16 mb-0"
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
        className="flex flex-row max-h-10 px-3 gap-2"
        >
          <button
            onClick={async () => {
              await navigator.clipboard.writeText(reasoning);
              setQueryLogCopied(true); 
              setTimeout(() => setQueryLogCopied(false), 3000); // Reset after 3 seconds
            }}
            className={`text-[1.3rem] font-bold text-main flex flex-row mt-1 hover:text-third opacity-0 pointer-events-none group-hover:opacity-100 group-hover:pointer-events-auto transition-opacity duration-300 ${
              queryLogCopied 
              ? "opacity-100"
              : reasoning.length === 0
                ? "opacity-0 pointer-events-none"
                :  ""
            }`}
          >
            ⧉
          </button>
          <div className={`text-[0.6rem] text-main opacity-0 pointer-events-none transition-opacity duration-300 flex flex-row mt-[1.6vh] ${
            queryLogCopied 
            ? "opacity-100" : ""
            }`}
          >
            <span>log copied to clipboard</span>
            </div>

        </div>

        {/* <div
        className="flex flex-row flex-1 min-h-1 w-8"
        >
          
        </div> */}

      </div>
         
    </div>
    
  );
}
