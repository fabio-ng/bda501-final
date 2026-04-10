import { BrowserRouter, Routes, Route, Link } from "react-router-dom";
import Top100Table from "./components/Top100Table";
import WalletGraph from "./components/WalletGraph";

export default function App() {
  return (
    <BrowserRouter>
      <header>
        <h1>ETH Transaction Analytics</h1>
        <nav>
          <Link to="/">Top 100</Link>
        </nav>
      </header>
      <div className="container">
        <Routes>
          <Route path="/" element={<Top100Table />} />
          <Route path="/wallet/:address" element={<WalletGraph />} />
        </Routes>
      </div>
    </BrowserRouter>
  );
}
