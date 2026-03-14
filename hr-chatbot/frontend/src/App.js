import { useState, useRef, useEffect } from "react";

export default function Chat() {

  const [messages, setMessages] = useState([]);
  const [input, setInput] = useState("");
  const [isTyping, setIsTyping] = useState(false);
  const [reasoning, setReasoning] = useState("");

  const [hasStarted, setHasStarted] = useState(false);
  const [queryLogEnabled, setQueryLogEnabled] = useState(true);
  const [sidebarOpen, setSidebarOpen] = useState(false);

  const messagesEndRef = useRef(null);
  const eventSourceRef = useRef(null);
  const reasoningContainerRef = useRef(null);
  const inputRef = useRef(null);

  useEffect(() => {
    document.body.classList.add("overflow-hidden");
    return () => document.body.classList.remove("overflow-hidden");
  }, []);

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

  useEffect(() => {
    if (reasoningContainerRef.current) {
      reasoningContainerRef.current.scrollTop =
        reasoningContainerRef.current.scrollHeight;
    }
  }, [reasoning]);

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

  const sendMessage = () => {

    if (!input.trim()) return;

    const userMessage = input;

    setMessages(prev => [...prev, { type: "user", text: userMessage }]);
    setInput("");
    setReasoning("");
    setIsTyping(true);
    setHasStarted(true);

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

            setMessages(prev => {
              const updated = [...prev];
              const last = updated[updated.length - 1];

              if (!last || last.type !== "bot") {
                updated.push({ type: "bot", text: fullText.slice(0, i) });
              } else {
                last.text = fullText.slice(0, i);
              }

              return updated;
            });

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
    <div class="fixed inset-0 grid grid-cols-12 gap-4 pointer-events-none z-50">
      <div class="border-r border-red-500/20"></div>
      <div class="border-r border-red-500/20"></div>
      <div class="border-r border-red-500/20"></div>
      <div class="border-r border-red-500/20"></div>
      <div class="border-r border-red-500/20"></div>
      <div class="border-r border-red-500/20"></div>
      <div class="border-r border-red-500/20"></div>
      <div class="border-r border-red-500/20"></div>
      <div class="border-r border-red-500/20"></div>
      <div class="border-r border-red-500/20"></div>
      <div class="border-r border-red-500/20"></div>
      <div class="border-r border-red-500/20"></div>
    </div>
      {/* Query Toggle */}
      <div className="fixed top-8 right-8 z-50 inline-flex items-center gap-2">

        <label htmlFor="query-toggle" className="text-sm text-main cursor-pointer">
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
      
        <div className={`rounded-2xl p-3 flex flex-row gap-2 flex-col transition duration-2400" ${
          sidebarOpen && hasStarted
            ? "bg-main translate-y-0"
            : sidebarOpen && !hasStarted
              ? "bg-main"
              : !sidebarOpen && !hasStarted
                ? "bg-none translate-y-[-40%]"
                : ""
        }`}
        >
          <button className={`border border-gray-800 rounded-lg p-2 text-black font-semibold w-full transition ${
            sidebarOpen
              ? "bg-second hover:bg-third"
              : "max-w-[13vh] bg-third hover:bg-second"
          } ${
            !hasStarted
              ? "pointer-events-none opacity-0 absolute top-0 left-0"
              : "flex"
          }`}
          >
            + New Chat
          </button>
          <button
            onClick={() => setSidebarOpen(prev => !prev)}
            className={`flex ml-[0.2vh] rounded-lg text-lg ${
              sidebarOpen
              ? "hover:text-second"
              : "text-main hover:text-third"
            } ${
              !hasStarted
              ? "py-[-7]"
              : ""
            }`}
          >
            ☰
          </button>

        </div>

      </div>

      {/* Chat Container */}
      <div
        className={`flex flex-col max-w-5xl flex-1 items-center transition-all duration-700`}
      >

        <div
          className={`w-full transition-all duration-700 ${
            hasStarted
              ? "max-w-3xl mt-auto"
              : "max-w-5xl translate-y-[175%]"
          }`}
        >
        {!hasStarted && (
          <div className="text-center mb-10 select-none">

            <h1 className="text-5xl text-main mb-3">
              Ask About HR Analytics
            </h1>

          </div>
        )}
          {/* Messages */}
          <div
            className="flex flex-col space-y-3 px-6"
            style={{
              maxHeight: "calc(100vh - 150px)",
              overflowY: hasStarted ? "auto" : "visible",
            }}
          >

            {messages.map((msg, i) => (
              <div
                key={i}
                className={`p-3 rounded-xl max-w-md break-words transition-all duration-300 ${
                  msg.type === "user"
                    ? "bg-second text-black ml-auto"
                    : "bg-main text-black"
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
          <div className="p-4 flex w-full rounded-4xl bg-gray-800">

            <textarea
              ref={inputRef}
              rows={1}
              className={
                `flex-1 p-3 rounded-xl text-black bg-main resize-none overflow-hidden max-h-32
                focus:ring-2 focus:ring-third focus:outline-none placeholder-back transition-all duration-200
                ${input.length > 0 ? "mr-4" : ""}
                `}
              value={input}
              onChange={handleInput}
              onKeyDown={handleKeyDown}
              placeholder="Ask a question..."
            />

            {/* Send Button */}
            <button
              onClick={sendMessage}
              className={`overflow-hidden transition-all duration-700 flex items-center justify-center
              ${input.length > 0 ? "w-16 opacity-100" : "w-0 opacity-0"}
            `}
            >
              <div className="w-16 h-[46px] bg-second rounded-xl flex items-center justify-center active:scale-90">

                <svg
                  xmlns="http://www.w3.org/2000/svg"
                  viewBox="-1.95 -4.4 10 10"
                  fill="none"
                  strokeWidth={0.4}
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  className="stroke-main w-10 h-10"
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
        className={`transition-all duration-700 w-80 flex flex-col flex-initial ${
          queryLogEnabled && (reasoning.length > 0 || hasStarted)
            ? "opacity-100 translate-x-0"
            : hasStarted
              ? "opacity-0 translate-x-10 pointer-events-none"
              : "opacity-0 translate-x-0 pointer-events-none"
        }`}
      >

        <div
          ref={reasoningContainerRef}
          className="bg-main rounded-2xl p-4 overflow-y-auto min-h-16"
        >

          <h2 className="text-lg font-semibold mb-2 text-gray-900">
            Query Log
          </h2>

          <div className="text-xs text-gray-900">

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

      </div>

    </div>
  );
}
