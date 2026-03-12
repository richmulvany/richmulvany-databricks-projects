import { useState, useRef, useEffect } from "react";

export default function Chat() {
  const [messages, setMessages] = useState([
    { type: "bot", text: "Hi — ask me anything about HR policies." }
  ]);

  const [input, setInput] = useState("");
  const [isTyping, setIsTyping] = useState(false);
  const [reasoning, setReasoning] = useState("");

  const messagesEndRef = useRef(null);
  const abortControllerRef = useRef(null);
  const reasoningContainerRef = useRef(null);

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

  useEffect(() => {
    if (reasoningContainerRef.current) {
      reasoningContainerRef.current.scrollTop =
        reasoningContainerRef.current.scrollHeight;
    }
  }, [reasoning]);

  const sendMessage = async () => {
    if (!input.trim()) return;

    const userMessage = input;
    setMessages(prev => [...prev, { type: "user", text: userMessage }]);
    setInput("");
    setReasoning("");
    setIsTyping(true);

    const controller = new AbortController();
    abortControllerRef.current = controller;

    try {
      const res = await fetch("http://localhost:8000/ask_stream", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ question: userMessage }),
        signal: controller.signal
      });

      setMessages(prev => [...prev, { type: "bot", text: "" }]);

      const reader = res.body.getReader();
      const decoder = new TextDecoder();

      let botMessage = "";
      let buffer = "";

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split("\n");
        buffer = lines.pop();

        lines.forEach(line => {
          if (!line) return;

          const [prefix, ...rest] = line.split("|");
          const content = rest.join("|");

          if (prefix === "bot") {
            botMessage += content;
            setMessages(prev => {
              const updated = [...prev];
              updated[updated.length - 1].text = botMessage;
              return updated;
            });
          } else if (prefix === "reasoning") {
            setReasoning(prev => prev + content + "\n");
          }
        });
      }

    } catch (err) {
      if (err.name !== "AbortError") console.error(err);
    }

    setIsTyping(false);
  };

  const stopResponse = () => {
    if (abortControllerRef.current) {
      abortControllerRef.current.abort();
      setIsTyping(false);
    }
  };

  const handleKeyDown = (e) => {
    if (e.key === "Enter") sendMessage();
  };

  return (
    <div className="flex h-screen bg-gray-800 text-black p-6 gap-6">

      {/* Sidebar */}
      <div className="w-64 bg-main border border-back rounded-2xl p-4 flex flex-col gap-4">
        <h2 className="text-lg font-semibold mb-2 text-gray-800 text-center">HR Assistant</h2>
        <button className="bg-second hover:bg-third border border-back rounded-lg p-2 text-black font-semibold w-full">
          + New Chat
        </button>
      </div>

      {/* Chat */}
      <div className="flex flex-col flex-1 max-w-3xl bg-gray-800 rounded-2xl overflow-hidden">
        <div className="flex-1 overflow-y-auto p-6 space-y-3">
          {messages.map((msg, i) => (
            <div
              key={i}
              className={`p-3 rounded-xl max-w-md break-words ${
                msg.type === "user"
                  ? "bg-second text-black ml-auto"
                  : "bg-main text-black"
              }`}
            >
              {msg.text}
            </div>
          ))}

          {isTyping && (
            <div className="p-3 rounded-xl bg-second flex space-x-2 w-fit">
              <span className="w-2 h-2 bg-third rounded-full animate-bounce"></span>
              <span className="w-2 h-2 bg-third rounded-full animate-bounce delay-150"></span>
              <span className="w-2 h-2 bg-third rounded-full animate-bounce delay-300"></span>
            </div>
          )}

          <div ref={messagesEndRef} />
        </div>

        {/* Input */}
        <div className="p-4 flex rounded-2xl space-x-4 bg-gray-800">
          <input
            className="flex-1 p-3 rounded-xl text-black bg-main focus:ring-2 focus:ring-third focus:outline-none placeholder-back"
            autoFocus
            value={input}
            onChange={e => setInput(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder="Ask a question..."
          />

        {isTyping ? (
          <button
            onClick={stopResponse}
            /* Fixed width (w-16) and group class added */
            className="w-16 h-[46px] bg-third rounded-xl flex items-center justify-center group hover:bg-second transition-colors, focus:ring-2 focus:ring-second focus:outline-none"
          >
            {/* group-hover:bg-third inverts the square color when the button is hovered */}
            <div className="w-4 h-4 bg-second rounded-sm group-hover:bg-third transition-colors, focus:ring-2 focus:ring-second  focus:outline-none"></div>
          </button>
        ) : (
          <button
            onClick={sendMessage}
            /* Same fixed width (w-16) and height matching */
            className="w-16 h-[46px] bg-second rounded-xl hover:bg-third text-black font-semibold transition-colors, focus:ring-2 focus:ring-third focus:outline-none"
          >
                {/* Upwards Arrow Icon */}
            <svg
              xmlns="http://www.w3.org/2000/svg"
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              strokeWidth={2.2}
              strokeLinecap="round"
              strokeLinejoin="round"
              className="w-5 h-5"
            >
              <path d="M4 12h11" />
              <path d="M11 7l5 5-5 5" />
              <path d="M20 5v6a3 3 0 0 1-3 3h-4" opacity="0.5"/>
            </svg>
          </button>
        )}
        </div>
      </div>

      {/* Reasoning panel */}
      <div className="w-80">
        <div
          ref={reasoningContainerRef}
          className="bg-main border border-back rounded-2xl p-4 overflow-y-auto h-full"
        >
          <h2 className="text-lg font-semibold mb-2 text-gray-900">Reasoning Log</h2>
          <pre className="text-sm text-gray-9 00 whitespace-pre-wrap">{reasoning}</pre>
        </div>
      </div>

    </div>
  );
}