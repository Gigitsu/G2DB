describe 'database' do
  def run_script(cmds)
    raw_output = nil
    IO.popen('./cmake-build-debug/G2DB', 'r+') do |pipe|
      cmds.each do |cmd|
        pipe.puts cmd
      end

      pipe.close_write

      raw_output = pipe.gets(nil)
    end
    raw_output.split("\n")
  end

  it 'inserts and retrieves a row' do
    result = run_script([
      'insert 1 gigitsu gigitsu.23@gmail.com',
      'select',
      '.exit'
    ])

    expect(result).to eq([
      'g2db> Executed.',
      'g2db> (1, gigitsu, gigitsu.23@gmail.com)',
      'Executed.',
      'g2db> '
    ])
  end

  it 'prints error message when table is full' do
    script = (1..1401).map do |i|
      "insert #{i} user#{i} email#{i}@g2.com"
    end

    script << '.exit'
    result = run_script(script)
    expect(result[-2]).to eq('g2db> Error: Table full.')
  end

  it 'allows inserting strings that are the maximum length' do
    long_username = 'g'*32
    long_email = 'g'*255
    
    script = [
      "insert 1 #{long_username} #{long_email}",
      "select",
      ".exit"
    ]

    result = run_script(script)
    expect(result).to eq([
      "g2db> Executed.",
      "g2db> (1, #{long_username}, #{long_email})",
      "Executed.",
      "g2db> "
    ])
  end

  it 'prints an error message if strings are too long' do
    long_username = 'g'*33
    long_email = 'g'*256

    script = [
      "insert 1 #{long_username} #{long_email}",
      "select",
      ".exit"
    ]

    result = run_script(script)
    expect(result).to eq([
      'g2db> String is too long.',
      'g2db> Executed.',
      'g2db> '
    ])
  end

  it 'prints an error message if id is negative' do
    script = [
      'insert -1 gigitsu gigitsu.23@gmail.com',
      'select',
      '.exit'
    ]

    result = run_script(script)

    expect(result).to eq([
      'g2db> ID must be positive.',
      'g2db> Executed.',
      'g2db> '
    ])
  end

  it 'keeps data after closing connection' do
    result1 = run_script([
      "insert 1 gigitsu gigitsu.23@gmail.com",
      ".exit",
    ])
    expect(result1).to eq([
      "g2db > Executed.",
      "g2db > ",
    ])
    result2 = run_script([
      "select",
      ".exit",
    ])
    expect(result2).to eq([
      "g2db > (1, gigitsu, gigitsu.23@gmail.com)",
      "Executed.",
      "g2db > ",
    ])
  end
end
