import { useState, useRef, useEffect } from "react";

export default function Chat() {

  const [messages, setMessages] = useState([
    { type: "bot", text: "Hi — ask me about HR data!" }
  ]);

  const [input, setInput] = useState("");
  const [isTyping, setIsTyping] = useState(false);
  const [reasoning, setReasoning] = useState("");

  const messagesEndRef = useRef(null);
  const eventSourceRef = useRef(null);
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

  const sendMessage = () => {

    if (!input.trim()) return;

    const userMessage = input;

    setMessages(prev => [...prev, { type: "user", text: userMessage }]);

    setInput("");
    setReasoning("");
    setIsTyping(true);

    const url =
      `http://localhost:8000/ask_stream?question=${encodeURIComponent(userMessage)}`;

    const source = new EventSource(url);

    eventSourceRef.current = source;

    source.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);

        // Stream reasoning
        if (data.trace) {
          setReasoning(prev => prev + data.trace + "\n");
        }

        // Stream bot message gradually
        if (data.answer) {
          const fullText = data.answer;
          let i = 0;

          const typeInterval = setInterval(() => {
            i++;
            setMessages(prev => {
              // replace last bot message or append if none
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
          }, 20); // 20ms per character

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
    if (e.key === "Enter") sendMessage();
  };

  return (

    <div className="flex h-screen bg-gray-800 text-black p-6 gap-6">

      <div className="w-64 bg-main border border-back rounded-2xl p-4 flex flex-col gap-4">

        <h2 className="text-lg font-semibold mb-2 text-gray-800 text-center">
          HR Assistant
        </h2>

        <button className="bg-second hover:bg-third border border-back rounded-lg p-2 text-black font-semibold w-full">
          + New Chat
        </button>

      </div>

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

            <div className="p-3 rounded-xl bg-main flex space-x-2 w-fit">

              <span className="w-2 h-2 bg-third border border-back rounded-full animate-bounce"></span>
              <span className="w-2 h-2 bg-third border border-back rounded-full animate-bounce delay-150"></span>
              <span className="w-2 h-2 bg-third border border-back rounded-full animate-bounce delay-300"></span>

            </div>

          )}

          <div ref={messagesEndRef} />

        </div>

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
              className="w-16 h-[46px] bg-third rounded-xl flex items-center justify-center group hover:bg-second transition-colors"
            >

              <div className="w-4 h-4 bg-second rounded-sm group-hover:bg-third transition-colors"></div>

            </button>

          ) : (

            <button
              onClick={sendMessage}
              className="w-16 h-[46px] bg-second rounded-xl hover:bg-third text-black font-semibold transition-colors"
            >

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

      <div className="w-80">

        <div
          ref={reasoningContainerRef}
          className="bg-main border border-back rounded-2xl p-4 overflow-y-auto h-full"
        >

          <h2 className="text-lg font-semibold mb-6 text-gray-900">
            Query Log
          </h2>

          <div className="text-sm text-gray-900">
            {reasoning.split("\n").map((line, idx) => {
              if (line.startsWith("###### ")) {
                return <h5 key={idx} className="font-semibold text-gray-700">{line.replace("###### ", "")}</h5>;
              }
              return <div key={idx}>{line}</div>;
            })}
          </div>

        </div>

      </div>

    </div>
  );
}