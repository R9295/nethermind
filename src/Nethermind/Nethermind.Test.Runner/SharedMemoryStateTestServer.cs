// SPDX-FileCopyrightText: 2026 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using Ethereum.Test.Base;
using Nethermind.Core;
using Nethermind.Core.Crypto;
using Nethermind.Evm;
using Nethermind.Evm.Tracing;
using Nethermind.Evm.TransactionProcessing;
using Nethermind.Serialization.Rlp;

namespace Nethermind.Test.Runner;

/// <summary>
/// Long-lived state-test server. Polls a sibling .signal file for START/EXIT,
/// runs the state test in <see cref="_dataPath"/>, and writes the bare hex
/// state root (or error string) back, plus OK/FAIL to the signal file.
/// </summary>
internal sealed class SharedMemoryStateTestServer : GeneralStateTestBase
{
    private const string SignalStart = "START";
    private const string SignalExit = "EXIT";
    private const string SignalOk = "OK";
    private const string SignalFail = "FAIL";

    private static readonly TimeSpan PollInterval = TimeSpan.FromMicroseconds(100);

    private readonly string _dataPath;
    private readonly string _signalPath;
    private readonly ulong _chainId;

    public SharedMemoryStateTestServer(string dataPath, ulong chainId)
    {
        ArgumentNullException.ThrowIfNull(dataPath);
        _dataPath = dataPath;
        _signalPath = dataPath + ".signal";
        _chainId = chainId;
        Setup(null);
    }

    public void Run(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            string signal = ReadSignal();
            switch (signal)
            {
                case SignalExit:
                    return;
                case SignalStart:
                    HandleOneTest();
                    break;
                default:
                    cancellationToken.WaitHandle.WaitOne(PollInterval);
                    break;
            }
        }
    }

    private void HandleOneTest()
    {
        try
        {
            (Hash256 root, Hash256 logsHash) = ExecuteCurrentTest();
            AtomicWrite(_dataPath, root.ToString() + "\n" + logsHash.ToString() + "\n");
            AtomicWrite(_signalPath, SignalOk);
        }
        catch (Exception e)
        {
            AtomicWrite(_dataPath, e.ToString());
            AtomicWrite(_signalPath, SignalFail);
        }
    }

    private (Hash256 stateRoot, Hash256 logsHash) ExecuteCurrentTest()
    {
        if (!File.Exists(_dataPath))
        {
            throw new FileNotFoundException("test file does not exist", _dataPath);
        }

        TestsSourceLoader source = new(new LoadGeneralStateTestFileStrategy(), _dataPath);
        IEnumerable<GeneralStateTest> tests = source.LoadTests<GeneralStateTest>();

        Hash256? agreedRoot = null;
        Hash256? agreedLogs = null;
        int matched = 0;
        foreach (GeneralStateTest test in tests)
        {
            test.ChainId = _chainId;
            LogsCollectorTracer tracer = new();
            EthereumTestResult result = RunTest(test, tracer);
            if (result.LoadFailure is not null)
            {
                throw new InvalidDataException(result.LoadFailure);
            }
            Hash256 logsHash = HashLogs(tracer.Logs);
            if (agreedRoot is null)
            {
                agreedRoot = result.StateRoot;
                agreedLogs = logsHash;
            }
            else if (!agreedRoot.Equals(result.StateRoot) || !agreedLogs!.Equals(logsHash))
            {
                throw new InvalidOperationException(
                    "multiple subtests with differing roots — please filter input");
            }
            matched++;
        }

        if (matched == 0 || agreedRoot is null || agreedLogs is null)
        {
            throw new InvalidOperationException("no matching subtest");
        }
        return (agreedRoot, agreedLogs);
    }

    private static Hash256 HashLogs(LogEntry[] logs)
    {
        Rlp encoded = Rlp.Encode(logs);
        return Keccak.Compute(encoded.Bytes);
    }

    private sealed class LogsCollectorTracer : TxTracer
    {
        public LogEntry[] Logs { get; private set; } = [];

        public LogsCollectorTracer() => IsTracingReceipt = true;

        public override void MarkAsSuccess(Address recipient, in GasConsumed gasSpent, byte[] output, LogEntry[] logs, Hash256? stateRoot = null) =>
            Logs = logs ?? [];

        public override void MarkAsFailed(Address recipient, in GasConsumed gasSpent, byte[] output, string? error, Hash256? stateRoot = null) =>
            Logs = [];
    }

    private string ReadSignal()
    {
        try
        {
            return File.ReadAllText(_signalPath).Trim();
        }
        catch (FileNotFoundException)
        {
            return string.Empty;
        }
        catch (DirectoryNotFoundException)
        {
            return string.Empty;
        }
    }

    private static void AtomicWrite(string path, string content)
    {
        string tmp = path + ".tmp";
        File.WriteAllText(tmp, content);
        File.Move(tmp, path, overwrite: true);
    }
}
